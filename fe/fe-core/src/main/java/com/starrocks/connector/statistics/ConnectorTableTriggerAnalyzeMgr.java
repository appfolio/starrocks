// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticExecutor;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectorTableTriggerAnalyzeMgr {
    private static final Logger LOG = LogManager.getLogger(ConnectorTableTriggerAnalyzeMgr.class);

    private final ConnectorAnalyzeTaskQueue connectorAnalyzeTaskQueue = new ConnectorAnalyzeTaskQueue();
    private final Map<ConnectorTableColumnKey, Optional<String>> keyToFileForGlobalDict = new ConcurrentHashMap<>();
    private final ScheduledExecutorService dispatchScheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isStart = new AtomicBoolean(false);

    public void start() {
        if (isStart.compareAndSet(false, true)) {
            dispatchScheduler.scheduleAtFixedRate(this::schedulePendingTask, 0,
                    Config.connector_table_query_trigger_task_schedule_interval, TimeUnit.SECONDS);
        }
    }

    // Any exception escaping from here would silently cancel the periodic schedule of scheduleAtFixedRate,
    // which stops both query triggered analyze and global dict update until FE restarts.
    @VisibleForTesting
    void schedulePendingTask() {
        try {
            if (GlobalStateMgr.getCurrentState().isLeader()) {
                connectorAnalyzeTaskQueue.schedulePendingTask();
            }
        } catch (Throwable e) {
            LOG.warn("[ExternalStats] schedule pending task failed", e);
        }

        try {
            scheduleDictUpdate();
        } catch (Throwable e) {
            LOG.warn("[ExternalStats] schedule dict update failed", e);
        }
    }

    private void scheduleDictUpdate() {
        for (Map.Entry<ConnectorTableColumnKey, Optional<String>> entry : keyToFileForGlobalDict.entrySet()) {
            Optional<String> value = keyToFileForGlobalDict.remove(entry.getKey());
            String tableUUID = entry.getKey().tableUUID;
            String columnName = entry.getKey().column;
            Runnable task = () -> {
                StatisticExecutor.updateDictSync(tableUUID, columnName, value);
            };
            try {
                ThreadPoolManager.getStatsCacheThreadPoolForLake().submit(task);
            } catch (RejectedExecutionException e) {
                keyToFileForGlobalDict.put(entry.getKey(), value);
                break;
            }
        }
    }

    public void checkAndUpdateTableStats(Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columnStats) {
        if (columnStats == null || columnStats.isEmpty()) {
            return;
        }

        // Decide which columns are due from the statistics already in hand, before touching the table.
        // This runs for every statistics lookup the leader makes while planning, and resolving an Iceberg
        // table outside the metadata cache reads its metadata.json from object storage (and, in this
        // cache-only scope, does not keep the result). Most lookups find fresh statistics, so resolve the
        // table only when an analyze will actually be queued.
        Set<String> analyzeColumns = Sets.newHashSet();
        String tableUUID = null;
        LocalDateTime now = LocalDateTime.now();
        for (Map.Entry<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> entry : columnStats.entrySet()) {
            ConnectorTableColumnKey columnKey = entry.getKey();
            if (tableUUID == null) {
                tableUUID = columnKey.tableUUID;
            }
            if (isDue(entry.getValue(), now)) {
                analyzeColumns.add(columnKey.column);
            }
        }

        if (analyzeColumns.isEmpty()) {
            return;
        }

        Triple<String, Database, Table> tableTriple;
        try (ConnectContext.ContextScope scope = ConnectContext.enterOnlyReadIcebergCacheScope(ConnectContext.get())) {
            tableTriple = StatisticsUtils.getTableTripleByUUID(scope.getContext(), tableUUID);
            // check table could run analyze
            if (!tableTriple.getRight().isAnalyzableExternalTable()) {
                return;
            }
        } catch (Exception e) {
            LOG.warn("[ExternalStats] trigger skip | table_uuid={} reason=table_not_found", tableUUID);
            return;
        }

        this.connectorAnalyzeTaskQueue.addPendingTask(tableUUID, new ConnectorAnalyzeTask(tableTriple, analyzeColumns));
    }

    private static boolean isDue(Optional<ConnectorTableColumnStats> columnStatsOptional, LocalDateTime now) {
        if (columnStatsOptional.isEmpty()) {
            return true;
        }
        ConnectorTableColumnStats columnStatsValue = columnStatsOptional.get();
        if (columnStatsValue.getUpdateTime() == null) {
            // No timestamp means no interval the column can be inside, so it is due. Parsing it would
            // throw and abandon every other column of the table along with it.
            return true;
        }
        LocalDateTime lastUpdateTime = DateUtils.parseStrictDateTime(columnStatsValue.getUpdateTime());
        long timeInterval = columnStatsValue.getRowCount() < Config.connector_table_query_trigger_analyze_small_table_rows ?
                Config.connector_table_query_trigger_analyze_small_table_interval :
                Config.connector_table_query_trigger_analyze_large_table_interval;
        return !lastUpdateTime.plusSeconds(timeInterval).isAfter(now);
    }

    public void addDictUpdateTask(ConnectorTableColumnKey key, Optional<String> fileName) {
        // the subsequent file will invalid previous ones
        Optional<String> old = keyToFileForGlobalDict.get(key);
        if (old == null || old.isPresent()) {
            keyToFileForGlobalDict.put(key, fileName);
        }
    }
}
