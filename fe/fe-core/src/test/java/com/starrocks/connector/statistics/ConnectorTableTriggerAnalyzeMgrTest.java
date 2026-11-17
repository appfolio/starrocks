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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.utframe.UtFrameUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.ImmutableTriple;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ConnectorTableTriggerAnalyzeMgrTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testScheduleSurvivesQueueException() throws Exception {
        AtomicInteger queueScheduleCount = new AtomicInteger();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        new MockUp<ConnectorAnalyzeTaskQueue>() {
            @Mock
            public void schedulePendingTask() {
                queueScheduleCount.incrementAndGet();
                throw new RuntimeException("mock schedule failure");
            }
        };
        CountDownLatch dictUpdated = new CountDownLatch(1);
        new MockUp<StatisticExecutor>() {
            @Mock
            public void updateDictSync(String tableUUID, String columnName, Optional<String> fileName) {
                dictUpdated.countDown();
            }
        };

        ConnectorTableTriggerAnalyzeMgr mgr = new ConnectorTableTriggerAnalyzeMgr();
        mgr.addDictUpdateTask(new ConnectorTableColumnKey("table_uuid", "col"), Optional.of("file"));

        // the exception from queue scheduling must not escape, otherwise scheduleAtFixedRate stops forever,
        // and dict update must still be scheduled in the same round
        Assertions.assertDoesNotThrow(mgr::schedulePendingTask);
        Assertions.assertEquals(1, queueScheduleCount.get());
        Assertions.assertTrue(dictUpdated.await(30, TimeUnit.SECONDS));

        Assertions.assertDoesNotThrow(mgr::schedulePendingTask);
        Assertions.assertEquals(2, queueScheduleCount.get());
    }

    private static final String TABLE_UUID = "iceberg_catalog.db.tbl.uuid";

    // Records how often the table was resolved and which columns were queued. The table resolves as
    // tableType, or is missing when tableType is null.
    private static class TriggerProbe {
        final AtomicInteger tableLookups = new AtomicInteger();
        final List<Set<String>> queuedColumns = new ArrayList<>();
    }

    private static TriggerProbe mockTableAndQueue(Table.TableType tableType) {
        TriggerProbe probe = new TriggerProbe();
        new MockUp<StatisticsUtils>() {
            @Mock
            public Triple<String, Database, Table> getTableTripleByUUID(ConnectContext context, String tableUUID) {
                probe.tableLookups.incrementAndGet();
                if (tableType == null) {
                    throw new RuntimeException("table dropped");
                }
                return ImmutableTriple.of("iceberg_catalog", new Database(1, "db"), new Table(tableType));
            }
        };
        new MockUp<ConnectorAnalyzeTaskQueue>() {
            @Mock
            public boolean addPendingTask(String tableUUID, ConnectorAnalyzeTask task) {
                probe.queuedColumns.add(task.getColumns());
                return true;
            }
        };
        return probe;
    }

    private static Optional<ConnectorTableColumnStats> statsUpdated(LocalDateTime updateTime, long rowCount) {
        return Optional.of(new ConnectorTableColumnStats(ColumnStatistic.unknown(), rowCount,
                updateTime == null ? null : DateUtils.formatDateTimeUnix(updateTime)));
    }

    private static Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columns(
            Map<String, Optional<ConnectorTableColumnStats>> byColumn) {
        return byColumn.entrySet().stream().collect(Collectors.toMap(
                e -> new ConnectorTableColumnKey(TABLE_UUID, e.getKey()), Map.Entry::getValue));
    }

    @Test
    public void testFreshStatisticsDoNotResolveTable() {
        TriggerProbe probe = mockTableAndQueue(Table.TableType.ICEBERG);
        LocalDateTime now = LocalDateTime.now();

        new ConnectorTableTriggerAnalyzeMgr().checkAndUpdateTableStats(columns(ImmutableMap.of(
                "small", statsUpdated(now.minusMinutes(10), 100),
                "large", statsUpdated(now.minusHours(6), 100_000_000L))));

        // Nothing is due, so the table (and its metadata.json) must not be loaded at all.
        Assertions.assertEquals(0, probe.tableLookups.get());
        Assertions.assertTrue(probe.queuedColumns.isEmpty());
    }

    @Test
    public void testDueColumnsResolveTableOnceAndQueueOnlyThem() {
        TriggerProbe probe = mockTableAndQueue(Table.TableType.ICEBERG);
        LocalDateTime now = LocalDateTime.now();

        new ConnectorTableTriggerAnalyzeMgr().checkAndUpdateTableStats(columns(ImmutableMap.of(
                "fresh", statsUpdated(now.minusMinutes(10), 100),
                "stale_small", statsUpdated(now.minusHours(3), 100),
                "stale_large", statsUpdated(now.minusHours(13), 100_000_000L),
                "missing", Optional.empty())));

        Assertions.assertEquals(1, probe.tableLookups.get());
        Assertions.assertEquals(1, probe.queuedColumns.size());
        Assertions.assertEquals(Set.of("stale_small", "stale_large", "missing"), probe.queuedColumns.get(0));
    }

    @Test
    public void testMissingUpdateTimeIsDueWithoutThrowing() {
        TriggerProbe probe = mockTableAndQueue(Table.TableType.ICEBERG);
        LocalDateTime now = LocalDateTime.now();

        Assertions.assertDoesNotThrow(() -> new ConnectorTableTriggerAnalyzeMgr().checkAndUpdateTableStats(
                columns(ImmutableMap.of(
                        "no_time", statsUpdated(null, 100),
                        "stale", statsUpdated(now.minusHours(3), 100)))));

        Assertions.assertEquals(1, probe.queuedColumns.size());
        Assertions.assertEquals(Set.of("no_time", "stale"), probe.queuedColumns.get(0));
    }

    @Test
    public void testDueColumnsOfNonAnalyzableTableAreNotQueued() {
        TriggerProbe probe = mockTableAndQueue(Table.TableType.VIEW);

        new ConnectorTableTriggerAnalyzeMgr().checkAndUpdateTableStats(
                columns(ImmutableMap.of("missing", Optional.empty())));

        Assertions.assertEquals(1, probe.tableLookups.get());
        Assertions.assertTrue(probe.queuedColumns.isEmpty());
    }

    @Test
    public void testDueColumnsOfMissingTableAreNotQueued() {
        TriggerProbe probe = mockTableAndQueue(null);

        Assertions.assertDoesNotThrow(() -> new ConnectorTableTriggerAnalyzeMgr().checkAndUpdateTableStats(
                columns(ImmutableMap.of("missing", Optional.empty()))));

        Assertions.assertEquals(1, probe.tableLookups.get());
        Assertions.assertTrue(probe.queuedColumns.isEmpty());
    }
}
