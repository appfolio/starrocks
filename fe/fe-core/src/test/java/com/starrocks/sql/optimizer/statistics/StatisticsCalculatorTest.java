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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalKuduScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class StatisticsCalculatorTest {
    private static ConnectContext connectContext;
    private static OptimizerContext optimizerContext;
    private static ColumnRefFactory columnRefFactory;
    private static StarRocksAssert starRocksAssert;
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = OptimizerFactory.mockContext(connectContext, columnRefFactory);

        starRocksAssert = new StarRocksAssert(connectContext);
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());

        String dbName = "statistics_test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        FeConstants.runningUnitTest = false;
    }

    @BeforeEach
    public void before() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_all_type` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "PARTITION BY RANGE (id_date)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"), \n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")  \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_all_type_day_partition` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "PARTITION BY RANGE (id_date)\n" +
                "(\n" +
                "partition p1 values [('2020-04-23'), ('2020-04-24')),\n" +
                "partition p2 values [('2020-04-24'), ('2020-04-25')),\n" +
                "partition p3 values [('2020-04-25'), ('2020-04-26')) \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @AfterEach
    public void after() throws Exception {
        starRocksAssert.dropTable("test_all_type");
        starRocksAssert.dropTable("test_all_type_day_partition");
    }

    @Test
    public void testLogicalAggregationRowCount() throws Exception {
        ColumnRefOperator v1 = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", IntegerType.INT, true);

        List<ColumnRefOperator> groupByColumns = Lists.newArrayList(v1);
        Map<ColumnRefOperator, CallOperator> aggCall = new HashMap<>();

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(10000);
        builder.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(0, 100, 0, 10, 50)));

        Group childGroup = new Group(0);
        childGroup.setStatistics(builder.build());

        LogicalAggregationOperator aggNode = new LogicalAggregationOperator(AggType.GLOBAL, groupByColumns, aggCall);
        GroupExpression groupExpression = new GroupExpression(aggNode, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(50, expressionContext.getStatistics().getOutputRowCount(), 0.001);

        groupByColumns = Lists.newArrayList(v1, v2);
        aggNode = new LogicalAggregationOperator(AggType.GLOBAL, groupByColumns, aggCall);
        groupExpression = new GroupExpression(aggNode, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(
                50 * 50 * Math.pow(StatisticsEstimateCoefficient.UNKNOWN_GROUP_BY_CORRELATION_COEFFICIENT, 2),
                expressionContext.getStatistics().getOutputRowCount(), 0.001);
    }

    @Test
    public void testLogicalUnion() throws Exception {
        // child 1 output column
        ColumnRefOperator v1 = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", IntegerType.INT, true);
        // child 2 output column
        ColumnRefOperator v3 = columnRefFactory.create("v3", IntegerType.INT, true);
        ColumnRefOperator v4 = columnRefFactory.create("v4", IntegerType.INT, true);
        // union node output column
        ColumnRefOperator v5 = columnRefFactory.create("v3", IntegerType.INT, true);
        ColumnRefOperator v6 = columnRefFactory.create("v4", IntegerType.INT, true);
        // child 1 statistics
        Statistics.Builder childBuilder1 = Statistics.builder();
        childBuilder1.setOutputRowCount(10000);
        childBuilder1.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        childBuilder1.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(0, 50, 0, 10, 50)));
        Group childGroup1 = new Group(0);
        childGroup1.setStatistics(childBuilder1.build());
        // child 2 statistics
        Statistics.Builder childBuilder2 = Statistics.builder();
        childBuilder2.setOutputRowCount(20000);
        childBuilder2.addColumnStatistics(ImmutableMap.of(v3, new ColumnStatistic(100, 200, 0, 10, 50)));
        childBuilder2.addColumnStatistics(ImmutableMap.of(v4, new ColumnStatistic(0, 100, 0, 10, 100)));
        Group childGroup2 = new Group(1);
        childGroup2.setStatistics(childBuilder2.build());
        // construct group expression
        LogicalUnionOperator unionOperator = new LogicalUnionOperator(Lists.newArrayList(v5, v6),
                Lists.newArrayList(Lists.newArrayList(v1, v2), Lists.newArrayList(v3, v4)), true);
        GroupExpression groupExpression =
                new GroupExpression(unionOperator, Lists.newArrayList(childGroup1, childGroup2));
        groupExpression.setGroup(new Group(2));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();

        ColumnStatistic columnStatisticV5 = expressionContext.getStatistics().getColumnStatistic(v5);
        ColumnStatistic columnStatisticV6 = expressionContext.getStatistics().getColumnStatistic(v6);
        Assertions.assertEquals(30000, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(new StatisticRangeValues(0, 200, 99), StatisticRangeValues.from(columnStatisticV5));
        Assertions.assertEquals(new StatisticRangeValues(0, 100, 100), StatisticRangeValues.from(columnStatisticV6));
    }

    @Test
    public void testLogicalOlapTableScan() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("statistics_test").getTable("test_all_type");
        Collection<Partition> partitions = table.getPartitions();
        List<Long> partitionIds =
                partitions.stream().mapToLong(partition -> partition.getId()).boxed().collect(Collectors.toList());
        for (Partition partition : partitions) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000);
        }

        List<Column> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
            Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
            Column column = columns.get(i);
            ColumnRefOperator ref = new ColumnRefOperator(i, column.getType(), column.getName(), true);
            refToColumn.put(ref, column);
            columnToRef.put(column, ref);

            LogicalOlapScanOperator olapScanOperator = new LogicalOlapScanOperator(table,
                    refToColumn, columnToRef,
                    null, -1, null,
                    ((OlapTable) table).getBaseIndexMetaId(),
                    partitionIds,
                    null,
                    false,
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    false);

            GroupExpression groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
            groupExpression.setGroup(new Group(0));
            ExpressionContext expressionContext = new ExpressionContext(groupExpression);
            Statistics.Builder builder = Statistics.builder();
            olapScanOperator.getOutputColumns().forEach(col ->
                    builder.addColumnStatistic(col,
                            new ColumnStatistic(-100, 100, 0.0, 5.0, 10))
            );
            expressionContext.setStatistics(builder.build());
            StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                    columnRefFactory, optimizerContext);
            statisticsCalculator.estimatorStats();
            Assertions.assertEquals(1000 * partitions.size(), expressionContext.getStatistics().getOutputRowCount(), 0.001);
            Assertions.assertEquals(ref.getType().getTypeSize() * 1000 * partitions.size(),
                    expressionContext.getStatistics().getComputeSize(), 0.001);
        }
    }

    @Test
    public void testLogicalIcebergTableScan() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Table icebergTable = globalStateMgr.getMetadataMgr().getTable(connectContext, "iceberg0", "partitioned_db", "t1");
        List<Column> columns = icebergTable.getColumns();

        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        ColumnRefOperator partitionColumn = null;
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            ColumnRefOperator ref = new ColumnRefOperator(i, column.getType(), column.getName(), true);
            if (column.getName().equals("date")) {
                partitionColumn = ref;
            }
            refToColumn.put(ref, column);
            columnToRef.put(column, ref);
        }

        BinaryPredicateOperator predicateOperator = new BinaryPredicateOperator(BinaryType.LT,
                partitionColumn, ConstantOperator.createInt(50));
        LogicalIcebergScanOperator icebergScanOperator = new LogicalIcebergScanOperator(icebergTable, refToColumn,
                columnToRef, -1, predicateOperator, TvrTableSnapshot.empty());

        GroupExpression groupExpression = new GroupExpression(icebergScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatisticsFromInternalStatistics(Table table, Map<ColumnRefOperator,
                    Column> columns) {
                Statistics.Builder builder = Statistics.builder();
                icebergScanOperator.getOutputColumns().forEach(col ->
                        builder.addColumnStatistic(col,
                                new ColumnStatistic(0, 100, 0.0, 5.0, 100))
                );
                builder.setOutputRowCount(100);
                return builder.build();
            }
        };
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(50, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(50, expressionContext.getStatistics().
                getColumnStatistic(partitionColumn).getMaxValue(), 0.001);
        Assertions.assertTrue(optimizerContext.isObtainedFromInternalStatistics());
        optimizerContext.setObtainedFromInternalStatistics(false);
    }

    @Test
    public void testLogicalOlapTableEmptyPartition() {
        ColumnRefOperator idDate = columnRefFactory.create("id_date", DateType.DATE, true);
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Table table = globalStateMgr.getLocalMetastore().getDb("statistics_test").getTable("test_all_type");

        List<Partition> partitions = new ArrayList<>(table.getPartitions());

        Partition partition1 = partitions.get(0);
        Partition partition2 = partitions.get(1);
        Partition partition3 = partitions.get(2);
        // mock one empty partition
        partition1.getDefaultPhysicalPartition().setVisibleVersion(Partition.PARTITION_INIT_VERSION, System.currentTimeMillis());
        partition1.getDefaultPhysicalPartition().setDataVersion(Partition.PARTITION_INIT_VERSION);
        partition2.getDefaultPhysicalPartition().setVisibleVersion(2, System.currentTimeMillis());
        partition2.getDefaultPhysicalPartition().setDataVersion(2);
        partition3.getDefaultPhysicalPartition().setVisibleVersion(2, System.currentTimeMillis());
        partition3.getDefaultPhysicalPartition().setDataVersion(2);
        List<Long> partitionIds = partitions.stream().filter(partition -> !(partition.getName().equalsIgnoreCase("p1"))).
                mapToLong(Partition::getId).boxed().collect(Collectors.toList());

        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                return List.of(new ColumnStatistic(0, Utils.getLongFromDateTime(
                        LocalDateTime.of(2014, 12, 01, 0, 0, 0)), 0, 0, 30));
            }
        };

        LogicalOlapScanOperator olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate),
                        null, -1, null,
                        ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);

        GroupExpression groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        ColumnStatistic columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);
        Assertions.assertEquals(30, columnStatistic.getDistinctValuesCount(), 0.001);

    }

    @Test
    public void testLogicalOlapTableScanPartitionPrune1() {
        FeConstants.runningUnitTest = true;
        ColumnRefOperator idDate = columnRefFactory.create("id_date", DateType.DATE, true);

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Table table = globalStateMgr.getLocalMetastore().getDb("statistics_test").getTable("test_all_type");

        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                return List.of(new ColumnStatistic(0, Utils.getLongFromDateTime(
                        LocalDateTime.of(2014, 12, 01, 0, 0, 0)), 0, 0, 30));
            }

            @Mock
            public ColumnStatistic getColumnStatistic(Table table, String column) {
                return new ColumnStatistic(0, Utils.getLongFromDateTime(
                        LocalDateTime.of(2014, 12, 01, 0, 0, 0)), 0, 0, 30);
            }
        };

        Collection<Partition> partitions = ((OlapTable) table).getPartitions();
        // select partition p1
        List<Long> partitionIds = partitions.stream().filter(partition -> partition.getName().equalsIgnoreCase("p1")).
                mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        for (Partition partition : partitions) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000);
        }

        LogicalOlapScanOperator olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate),
                        null, -1,
                        new BinaryPredicateOperator(BinaryType.EQ,
                                idDate, ConstantOperator.createDate(LocalDateTime.of(2013, 12, 30, 0, 0, 0))),
                        ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);

        GroupExpression groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        // partition column count distinct values is 30 in table level, after partition prune,
        // the column statistic distinct values is 10, so the estimate row count is 1000 * (1/10)
        Assertions.assertEquals(100, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        ColumnStatistic columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2013, 12, 30, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);

        // select partition p2, p3
        partitionIds.clear();
        partitionIds = partitions.stream().filter(partition -> !(partition.getName().equalsIgnoreCase("p1"))).
                mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate),
                        null, -1, null, ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);
        olapScanOperator.setPredicate(new BinaryPredicateOperator(BinaryType.GE,
                idDate, ConstantOperator.createDate(LocalDateTime.of(2014, 5, 1, 0, 0, 0))));

        groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);

        Assertions.assertEquals(1281.4371, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2014, 5, 1, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2014, 12, 1, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(20, columnStatistic.getDistinctValuesCount(), 0.001);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testLogicalOlapTableScanPartitionPrune2() {
        FeConstants.runningUnitTest = true;
        ColumnRefOperator idDate = columnRefFactory.create("id_date", DateType.DATE, true);

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("statistics_test")
                .getTable("test_all_type_day_partition");

        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                return List.of(new ColumnStatistic(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 23, 0, 0, 0)),
                        Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 25, 0, 0, 0)), 0, 0, 3));
            }

            @Mock
            public ColumnStatistic getColumnStatistic(Table table, String column) {
                return new ColumnStatistic(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 23, 0, 0, 0)),
                        Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 25, 0, 0, 0)), 0, 0, 3);
            }
        };

        Collection<Partition> partitions = table.getPartitions();
        // select partition p2
        List<Long> partitionIds = partitions.stream().filter(partition -> partition.getName().equalsIgnoreCase("p2")).
                mapToLong(partition -> partition.getId()).boxed().collect(Collectors.toList());
        for (Partition partition : partitions) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000);
        }

        LogicalOlapScanOperator olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate), null, -1, null,
                        ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);

        GroupExpression groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();

        Assertions.assertEquals(1000, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        ColumnStatistic columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 24, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 25, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(1, columnStatistic.getDistinctValuesCount(), 0.001);

        // select partition p2, p3
        partitionIds.clear();
        partitionIds = partitions.stream().filter(partition -> !(partition.getName().equalsIgnoreCase("p1"))).
                mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate), null, -1, null,
                        ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);
        olapScanOperator.setPredicate(new BinaryPredicateOperator(BinaryType.GE,
                idDate, ConstantOperator.createDate(LocalDateTime.of(2020, 04, 24, 0, 0, 0))));

        groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);
        // has two partitions
        Assertions.assertEquals(2000, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 24, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 26, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(2, columnStatistic.getDistinctValuesCount(), 0.001);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testPredicateOnColumnPinnedToSingleValue() {
        // `d_year = 2000` narrows the range to one value but keeps its NDV; the same filter applied again, e.g. on a
        // CTE consumer, must keep every row instead of dividing by that NDV a second time
        Statistics input = Statistics.builder().setOutputRowCount(1000).build();
        ColumnStatistic pinned = new ColumnStatistic(2000, 2000, 0, 4, 202);
        Assertions.assertEquals(1000, BinaryPredicateStatisticCalculator.estimatePredicateRange(Optional.empty(),
                pinned, new StatisticRangeValues(2000, 2000, 1), input).getOutputRowCount(), 1e-6);
        // `col < 2000` / `col > 2000` are encoded as ranges including 2000 (with NaN NDV, as the LT / GT estimators
        // build them); they must not keep the pinned rows
        Assertions.assertEquals(1000.0 / 202, BinaryPredicateStatisticCalculator.estimatePredicateRange(
                Optional.empty(), pinned, new StatisticRangeValues(Double.NEGATIVE_INFINITY, 2000, Double.NaN), input)
                .getOutputRowCount(), 1e-6);
        Assertions.assertEquals(1000.0 / 202, BinaryPredicateStatisticCalculator.estimatePredicateRange(
                Optional.empty(), pinned, new StatisticRangeValues(2000, Double.POSITIVE_INFINITY, Double.NaN), input)
                .getOutputRowCount(), 1e-6);
        // outside the pinned value: nothing is kept (the row count is floored at 1)
        Assertions.assertTrue(BinaryPredicateStatisticCalculator.estimatePredicateRange(Optional.empty(),
                pinned, new StatisticRangeValues(2001, 2001, 1), input).getOutputRowCount() <= 1);
        // a string constant has no finite bounds, and a string column's min = max is only a placeholder
        Assertions.assertEquals(1000.0 / 202, BinaryPredicateStatisticCalculator.estimatePredicateRange(
                Optional.empty(), pinned, new StatisticRangeValues(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                        1), input).getOutputRowCount(), 1e-6);
        // a real range still keeps 1 / NDV of its rows for a single value
        ColumnStatistic spread = new ColumnStatistic(1900, 2100, 0, 4, 202);
        Assertions.assertEquals(1000.0 / 202, BinaryPredicateStatisticCalculator.estimatePredicateRange(
                Optional.empty(), spread, new StatisticRangeValues(2000, 2000, 1), input).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testJoinEstimateWithMultiColumns() {
        // child 1 output column
        ColumnRefOperator v1 = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", IntegerType.INT, true);
        ColumnRefOperator v5 = columnRefFactory.create("v5", IntegerType.INT, true);
        // child 2 output column
        ColumnRefOperator v3 = columnRefFactory.create("v3", IntegerType.INT, true);
        ColumnRefOperator v4 = columnRefFactory.create("v4", IntegerType.INT, true);
        ColumnRefOperator v6 = columnRefFactory.create("v6", IntegerType.INT, true);
        // child 1 statistics
        Statistics.Builder childBuilder1 = Statistics.builder();
        childBuilder1.setOutputRowCount(10000);
        childBuilder1.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        childBuilder1.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(0, 50, 0, 10, 50)));
        childBuilder1.addColumnStatistics(ImmutableMap.of(v5, new ColumnStatistic(0, 50, 0, 10, 50)));
        Group childGroup1 = new Group(0);
        childGroup1.setStatistics(childBuilder1.build());
        childGroup1.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(v1, v2, v5))));
        // child 2 statistics
        Statistics.Builder childBuilder2 = Statistics.builder();
        childBuilder2.setOutputRowCount(20000);
        childBuilder2.addColumnStatistics(ImmutableMap.of(v3, new ColumnStatistic(100, 200, 0, 10, 50)));
        childBuilder2.addColumnStatistics(ImmutableMap.of(v4, new ColumnStatistic(0, 100, 0, 10, 100)));
        childBuilder2.addColumnStatistics(ImmutableMap.of(v6, new ColumnStatistic(0, 100, 0, 10, 100)));
        Group childGroup2 = new Group(1);
        childGroup2.setStatistics(childBuilder2.build());
        childGroup2.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(v3, v4, v6))));

        // record column id to relation id
        columnRefFactory.updateColumnToRelationIds(v1.getId(), 0);
        columnRefFactory.updateColumnToRelationIds(v2.getId(), 0);
        columnRefFactory.updateColumnToRelationIds(v3.getId(), 1);
        columnRefFactory.updateColumnToRelationIds(v4.getId(), 1);
        columnRefFactory.updateColumnToRelationIds(v5.getId(), 3);
        columnRefFactory.updateColumnToRelationIds(v6.getId(), 4);
        // on predicate : t0.v1 = t1.v3 and t0.v2 = t1.v4
        BinaryPredicateOperator eqOnPredicate1 =
                new BinaryPredicateOperator(BinaryType.EQ, v1, v3);
        BinaryPredicateOperator eqOnPredicate2 =
                new BinaryPredicateOperator(BinaryType.EQ, v2, v4);
        BinaryPredicateOperator eqOnPredicate3 =
                new BinaryPredicateOperator(BinaryType.EQ, v5, v6);
        // construct group expression
        LogicalJoinOperator joinOperator =
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND, eqOnPredicate1, eqOnPredicate2));
        GroupExpression groupExpression =
                new GroupExpression(joinOperator, Lists.newArrayList(childGroup1, childGroup2));
        groupExpression.setGroup(new Group(2));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        // use middle ground method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(400000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);
        // use correlated method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(true);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(1800000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);

        // on predicate : t0.v2 = t1.v4 and t0.v1 = t1.v3, the same predicates in the other order: middle ground
        // applies the larger selectivity first whatever the order, so the estimate must not change
        GroupExpression swappedExpression = new GroupExpression(
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND, eqOnPredicate2, eqOnPredicate1)),
                Lists.newArrayList(childGroup1, childGroup2));
        swappedExpression.setGroup(new Group(2));
        ExpressionContext swappedContext = new ExpressionContext(swappedExpression);
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        new StatisticsCalculator(swappedContext, columnRefFactory, optimizerContext).estimatorStats();
        Assertions.assertEquals(400000.0, swappedContext.getStatistics().getOutputRowCount(), 0.0001);

        // on predicate : t0.v1 = t1.v3 and t0.v2 = t2.v4
        columnRefFactory.updateColumnToRelationIds(v4.getId(), 2);
        // use middle ground method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(40000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);
        columnRefFactory.updateColumnToRelationIds(v4.getId(), 1);

        // on predicate : t0.v1 = t1.v3 and t0.v2 = t2.v4 and t3.v5 = t4.v6
        // construct group expression
        joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, eqOnPredicate1, new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, eqOnPredicate2, eqOnPredicate3)));
        groupExpression = new GroupExpression(joinOperator, Lists.newArrayList(childGroup1, childGroup2));
        groupExpression.setGroup(new Group(2));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        // use middle ground method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(4000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);

        // on predicate : t0.v1 = t1.v3 + t1.v4 and t0.v2 = t1.v3 + t1.v4
        BinaryPredicateOperator eqOnPredicateWithAdd1 =
                new BinaryPredicateOperator(BinaryType.EQ, v1,
                        new CallOperator("add", IntegerType.BIGINT, Lists.newArrayList(v3, v4)));
        BinaryPredicateOperator eqOnPredicateWithAdd2 =
                new BinaryPredicateOperator(BinaryType.EQ, v2,
                        new CallOperator("add", IntegerType.BIGINT, Lists.newArrayList(v3, v4)));
        joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, eqOnPredicateWithAdd1, eqOnPredicateWithAdd2));
        groupExpression = new GroupExpression(joinOperator, Lists.newArrayList(childGroup1, childGroup2));
        groupExpression.setGroup(new Group(2));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        // use middle ground method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(200000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);
    }

    @Test
    public void testNotFoundColumnStatistics() {
        ColumnRefOperator v1 = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", IntegerType.INT, true);

        ColumnRefOperator v3 = columnRefFactory.create("v3", IntegerType.INT, true);
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(10000);
        builder.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(0, 100, 0, 10, 50)));
        Statistics statistics = builder.build();
        Assertions.assertThrows(StarRocksPlannerException.class, () -> statistics.getColumnStatistic(v3));
    }

    public enum OuterJoin {
        LEFT_OUTER_JOIN,
        RIGHT_OUTER_JOIN
    }

    @ParameterizedTest
    @EnumSource(value = OuterJoin.class)
    public void testOuterJoinPreservesOuterSideNullFraction(OuterJoin outerJoin) {
        // GIVEN
        ColumnRefOperator outerJoinKey = columnRefFactory.create("nullable_col", IntegerType.INT, true);
        ColumnRefOperator outerOtherCol = columnRefFactory.create("other_col", IntegerType.INT, true);

        final double outerNullFraction = 0.755;
        Statistics.Builder outerBuilder = Statistics.builder();
        outerBuilder.setOutputRowCount(1000000);
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerJoinKey, new ColumnStatistic(1, 100000, outerNullFraction, 8, 50000)));
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerOtherCol, new ColumnStatistic(0, 100, 0.1, 8, 50)));

        Group outerGroup = new Group(0);
        outerGroup.setStatistics(outerBuilder.build());
        outerGroup.setLogicalProperty(new LogicalProperty(
                new ColumnRefSet(Lists.newArrayList(outerJoinKey, outerOtherCol))));

        ColumnRefOperator innerKey = columnRefFactory.create("dim_id", IntegerType.INT, true);
        ColumnRefOperator innerVal = columnRefFactory.create("dim_val", IntegerType.INT, true);

        Statistics.Builder innerBuilder = Statistics.builder();
        innerBuilder.setOutputRowCount(200000);
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerKey, new ColumnStatistic(1, 200000, 0, 8, 200000)));
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerVal, new ColumnStatistic(0, 100, 0, 8, 50)));

        Group innerGroup = new Group(1);
        innerGroup.setStatistics(innerBuilder.build());
        innerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(innerKey, innerVal))));

        // LEFT JOIN: outer.nullable_col = inner.dim1_id
        // RIGHT JOIN: inner.dim1_id = outer.nullable_col
        JoinOperator joinType;
        if (outerJoin == OuterJoin.LEFT_OUTER_JOIN) {
            joinType = JoinOperator.LEFT_OUTER_JOIN;
        } else {
            joinType = JoinOperator.RIGHT_OUTER_JOIN;
        }

        BinaryPredicateOperator joinPred;
        GroupExpression groupExpr;
        if (joinType == JoinOperator.LEFT_OUTER_JOIN) {
            joinPred = new BinaryPredicateOperator(BinaryType.EQ, outerJoinKey, innerKey);
        } else {
            joinPred = new BinaryPredicateOperator(BinaryType.EQ, innerKey, outerJoinKey);
        }

        LogicalJoinOperator joinOp = new LogicalJoinOperator(joinType, joinPred);

        if (joinType == JoinOperator.LEFT_OUTER_JOIN) {
            groupExpr = new GroupExpression(joinOp, Lists.newArrayList(outerGroup, innerGroup));
        } else {
            groupExpr = new GroupExpression(joinOp, Lists.newArrayList(innerGroup, outerGroup));
        }

        Group joinGroup = new Group(2);
        groupExpr.setGroup(joinGroup);
        ExpressionContext exprCtx = new ExpressionContext(groupExpr);
        StatisticsCalculator calc = new StatisticsCalculator(exprCtx, columnRefFactory, optimizerContext);

        // WHEN
        // The outer side join key's null fraction should be preserved after LEFT JOIN
        calc.estimatorStats();
        Statistics joinStats = exprCtx.getStatistics();
        ColumnStatistic outerJoinKeyStatAfterJoin = joinStats.getColumnStatistic(outerJoinKey);

        // THEN
        Assertions.assertEquals(outerNullFraction, outerJoinKeyStatAfterJoin.getNullsFraction(), 0.001,
                "Outer join key null fraction should be preserved after " + outerJoin.name());
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }

    @Test
    public void testSkipPredicateColumnsCollectionScopeIsNestableAndRestores() {
        Assertions.assertFalse(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());

        try (var scope1 = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
            Assertions.assertTrue(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());

            try (var scope2 = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
                Assertions.assertTrue(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());
            }

            Assertions.assertTrue(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());
        }

        Assertions.assertFalse(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());
    }

    @Test
    public void testSkipPredicateColumnsCollectionScopeDoesNotLeakAcrossThreads() throws Exception {
        Assertions.assertFalse(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();

        Thread t = new Thread(() -> {
            try (var scope = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
                entered.countDown();
                Assertions.assertTrue(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());
            } catch (Throwable e) {
                err.set(e);
            } finally {
                done.countDown();
            }
        });
        t.start();

        Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
        Assertions.assertFalse(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());

        Assertions.assertTrue(done.await(5, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    @Test
    public void testComputeEsScanNode(@Mocked EsTable esTable) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        LogicalEsScanOperator scanOperator =
                new LogicalEsScanOperator(esTable, refToColumn, columnToRef, -1, null, null);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                                 ScalarOperator predicate) {
                return Statistics.builder().setOutputRowCount(999_000).build();
            }
        };
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            public Statistics.Builder estimateScanColumns(Table table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                          OptimizerContext ctx) {
                return Statistics.builder();
            }
        };

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(999_000D, expressionContext.getStatistics().getOutputRowCount(), 0.01);
    }

    @Test
    public void testComputeKuduScanNode(@Mocked KuduTable kuduTable) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        LogicalKuduScanOperator scanOperator =
                new LogicalKuduScanOperator(kuduTable, refToColumn, columnToRef, -1, null);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                                 ScalarOperator predicate) {
                return Statistics.builder().setOutputRowCount(888_000).build();
            }
        };
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            public Statistics.Builder estimateScanColumns(Table table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                          OptimizerContext ctx) {
                return Statistics.builder();
            }
        };

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(888_000D, expressionContext.getStatistics().getOutputRowCount(), 0.01);
    }

    @Test
    public void testExternalScanPreservesConnectorForegroundSource(@Mocked KuduTable kuduTable) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        LogicalKuduScanOperator scanOperator =
                new LogicalKuduScanOperator(kuduTable, refToColumn, columnToRef, -1, null);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                                 ScalarOperator predicate) {
                return Statistics.builder().setOutputRowCount(100)
                        .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
            }
        };
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            public Statistics.Builder estimateScanColumns(Table table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                          OptimizerContext ctx) {
                return Statistics.builder();
            }
        };

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA,
                expressionContext.getStatistics().getStatsSource());
    }

    @Test
    public void testExternalScanDefaultUnknown(@Mocked KuduTable kuduTable) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        LogicalKuduScanOperator scanOperator =
                new LogicalKuduScanOperator(kuduTable, refToColumn, columnToRef, -1, null);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        // getTableStatistics returns null, triggering catch -> computeNormalExternalTableScanNode with UNKNOWN
        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                                 ScalarOperator predicate) {
                return null;
            }
        };
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            public Statistics.Builder estimateScanColumns(Table table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                          OptimizerContext ctx) {
                return Statistics.builder();
            }
        };

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(Statistics.StatsSource.NONE,
                expressionContext.getStatistics().getStatsSource());
    }

    @Test
    public void testStatsSourceDefaults() {
        // Default Builder produces UNKNOWN
        Statistics stats = Statistics.builder().setOutputRowCount(100).build();
        Assertions.assertEquals(Statistics.StatsSource.NONE, stats.getStatsSource());

        // Explicit set works
        Statistics foreground = Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, foreground.getStatsSource());

        Statistics background = Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.ANALYZE).build();
        Assertions.assertEquals(Statistics.StatsSource.ANALYZE, background.getStatsSource());
    }

    @Test
    public void testStatsSourceBuildFromPreservesSource() {
        Statistics original = Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        Statistics rebuilt = Statistics.buildFrom(original).setOutputRowCount(200).build();
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, rebuilt.getStatsSource());
        Assertions.assertEquals(200, rebuilt.getOutputRowCount(), 0.001);
    }

    @Test
    public void testStatsSourceWithOutputRowCountPreservesSource() {
        Statistics original = Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.ANALYZE).build();
        Statistics adjusted = original.withOutputRowCount(200);
        Assertions.assertEquals(Statistics.StatsSource.ANALYZE, adjusted.getStatsSource());
        Assertions.assertEquals(200, adjusted.getOutputRowCount(), 0.001);
    }

    @Test
    public void testOlapScanGetsBackground() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("statistics_test")
                .getTable("test_all_type");
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        for (Column col : table.getBaseSchema()) {
            ColumnRefOperator ref = columnRefFactory.create(col.getName(), col.getType(), col.isAllowNull());
            colRefToColumnMetaMap.put(ref, col);
            columnToRef.put(col, ref);
        }
        List<Long> partitionIds = table.getPartitions().stream()
                .mapToLong(Partition::getId).boxed().collect(Collectors.toList());

        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(table, colRefToColumnMetaMap,
                columnToRef, null, -1, null, table.getBaseIndexMetaId(),
                partitionIds, null, false, Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList(), false);

        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(Statistics.StatsSource.ANALYZE, expressionContext.getStatistics().getStatsSource());
    }

    private Statistics estimateJoin(JoinOperator joinType, Group left, Group right, ColumnRefOperator leftKey,
                                    ColumnRefOperator rightKey) {
        var groupExpr = new GroupExpression(new LogicalJoinOperator(joinType,
                new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey)), Lists.newArrayList(left, right));
        groupExpr.setGroup(new Group(2));
        var ctx = new ExpressionContext(groupExpr);
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        return ctx.getStatistics();
    }

    private static Group joinInput(int id, double rowCount, Map<ColumnRefOperator, ColumnStatistic> columns) {
        var group = new Group(id);
        group.setStatistics(Statistics.builder().setOutputRowCount(rowCount).addColumnStatistics(columns).build());
        group.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(columns.keySet()))));
        return group;
    }

    /**
     * An anti join outputs the preserved side's UNMATCHED rows, so its key keeps the preserved side's distinct
     * values. It used to take the inner join's min(ndv(left), ndv(right)), collapsing a 375,705-value key to the
     * other side's handful, and shrinking the other side made it worse.
     */
    @Test
    public void testAntiJoinKeepsPreservedKeyDistinctValues() {
        final var base = columnRefFactory.create("base_key", IntegerType.BIGINT, false);
        final var overlay = columnRefFactory.create("overlay_key", IntegerType.BIGINT, false);
        final var baseGroup = joinInput(0, 375705, ImmutableMap.of(base, new ColumnStatistic(1, 400000, 0, 8, 375705)));
        double previous = 0;
        for (double overlayRows : new double[] {1000, 100, 15, 1}) {
            final var overlayGroup = joinInput(1, overlayRows,
                    ImmutableMap.of(overlay, new ColumnStatistic(1, 400000, 0, 8, overlayRows)));
            for (JoinOperator joinType : new JoinOperator[] {
                    JoinOperator.LEFT_ANTI_JOIN, JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN}) {
                Statistics stats = estimateJoin(joinType, baseGroup, overlayGroup, base, overlay);
                double ndv = stats.getColumnStatistic(base).getDistinctValuesCount();
                Assertions.assertEquals(Math.min(375705, stats.getOutputRowCount()), ndv, 1, joinType + " " + overlayRows);
                Assertions.assertTrue(stats.getOutputRowCount() <= 375705, joinType + " " + overlayRows);
            }
            // Fewer rows to exclude never means fewer distinct values left.
            double ndv = estimateJoin(JoinOperator.LEFT_ANTI_JOIN, baseGroup, overlayGroup, base, overlay)
                    .getColumnStatistic(base).getDistinctValuesCount();
            Assertions.assertTrue(ndv >= previous, "overlay rows " + overlayRows);
            previous = ndv;

            // RIGHT_ANTI_JOIN is the mirror: the right side is preserved.
            Statistics right = estimateJoin(JoinOperator.RIGHT_ANTI_JOIN, overlayGroup, baseGroup, overlay, base);
            Assertions.assertEquals(Math.min(375705, right.getOutputRowCount()),
                    right.getColumnStatistic(base).getDistinctValuesCount(), 1, "right anti " + overlayRows);
        }
    }

    /**
     * A preserved-side column with unknown statistics must not leave the anti join with the cross product's row
     * count: the output is at most the preserved side's rows.
     */
    @Test
    public void testAntiJoinWithUnknownColumnKeepsItsRowCount() {
        final var base = columnRefFactory.create("base_key", IntegerType.BIGINT, false);
        final var unknown = columnRefFactory.create("base_unknown", IntegerType.BIGINT, true);
        final var overlay = columnRefFactory.create("overlay_key", IntegerType.BIGINT, false);
        final var baseGroup = joinInput(0, 100000, ImmutableMap.of(
                base, new ColumnStatistic(1, 100000, 0, 8, 100000), unknown, ColumnStatistic.unknown()));
        final var overlayGroup = joinInput(1, 50, ImmutableMap.of(overlay, new ColumnStatistic(1, 100000, 0, 8, 50)));
        Statistics stats = estimateJoin(JoinOperator.LEFT_ANTI_JOIN, baseGroup, overlayGroup, base, overlay);
        Assertions.assertTrue(stats.getOutputRowCount() <= 100000, String.valueOf(stats.getOutputRowCount()));
        Assertions.assertTrue(stats.getColumnStatistic(unknown).isUnknown());
        Assertions.assertTrue(stats.getColumnStatistic(base).getDistinctValuesCount() > 50);
    }

    /**
     * An outer join keeps every preserved-side row, so the preserved key keeps its own distinct values instead of
     * the inner join's min of the two sides. The nullable side's key keeps only its matched values.
     */
    @Test
    public void testOuterJoinPreservedKeyKeepsItsDistinctValues() {
        final var lk = columnRefFactory.create("lk", IntegerType.BIGINT, false);
        final var rk = columnRefFactory.create("rk", IntegerType.BIGINT, false);
        final var left = joinInput(0, 6876, ImmutableMap.of(lk, new ColumnStatistic(1, 10000, 0, 8, 6876)));
        final var right = joinInput(1, 40, ImmutableMap.of(rk, new ColumnStatistic(1, 10000, 0, 8, 33)));

        Statistics leftJoin = estimateJoin(JoinOperator.LEFT_OUTER_JOIN, left, right, lk, rk);
        Assertions.assertEquals(6876, leftJoin.getColumnStatistic(lk).getDistinctValuesCount(), 1);
        Assertions.assertEquals(1, leftJoin.getColumnStatistic(lk).getMinValue(), 0);
        Assertions.assertEquals(10000, leftJoin.getColumnStatistic(lk).getMaxValue(), 0);
        Assertions.assertTrue(leftJoin.getColumnStatistic(rk).getDistinctValuesCount() <= 33);

        Statistics rightJoin = estimateJoin(JoinOperator.RIGHT_OUTER_JOIN, right, left, rk, lk);
        Assertions.assertEquals(6876, rightJoin.getColumnStatistic(lk).getDistinctValuesCount(), 1);
        Assertions.assertTrue(rightJoin.getColumnStatistic(rk).getDistinctValuesCount() <= 33);

        // Both sides are preserved in a full outer join.
        Statistics fullJoin = estimateJoin(JoinOperator.FULL_OUTER_JOIN, left, right, lk, rk);
        Assertions.assertEquals(6876, fullJoin.getColumnStatistic(lk).getDistinctValuesCount(), 1);
        Assertions.assertEquals(33, fullJoin.getColumnStatistic(rk).getDistinctValuesCount(), 1);

        // A preserved non-key column keeps its own distinct values, not the matched row count.
        final var lv = columnRefFactory.create("lv", IntegerType.BIGINT, false);
        final var leftWithValue = joinInput(0, 6876, ImmutableMap.of(
                lk, new ColumnStatistic(1, 10000, 0, 8, 6876), lv, new ColumnStatistic(1, 5000, 0, 8, 3000)));
        Statistics withValue = estimateJoin(JoinOperator.LEFT_OUTER_JOIN, leftWithValue, right, lk, rk);
        Assertions.assertEquals(3000, withValue.getColumnStatistic(lv).getDistinctValuesCount(), 1);

        // An inner join still narrows both keys to the values they share.
        Statistics innerJoin = estimateJoin(JoinOperator.INNER_JOIN, left, right, lk, rk);
        Assertions.assertTrue(innerJoin.getColumnStatistic(lk).getDistinctValuesCount() <= 33);
    }
}
