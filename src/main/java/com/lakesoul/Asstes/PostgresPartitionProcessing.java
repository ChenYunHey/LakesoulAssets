
package com.lakesoul.Asstes;

import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.Properties;


public class PostgresPartitionProcessing {
    public static void main(String[] args) throws Exception {

        PgDeserialization deserialization = new PgDeserialization();
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("include.unknown.datatypes", "true");
        String[] tableList = new String[]{"public.partition_info","public.table_info","public.data_commit_info"};
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("localhost")
                        .port(5432)
                        .database("lakesoul_test")
                        .schemaList("public")
                        .tableList(tableList)
                        .username("lakesoul_test")
                        .password("lakesoul_test")
                        .slotName("flink")
                        .decodingPluginName("pgoutput") // use pgoutput for PostgreSQL 10+
                        .deserializer(deserialization)
                        .includeSchemaChanges(true) // output the schema changes as well
                        .splitSize(2) // the split size of each snapshot split
                        .debeziumProperties(debeziumProperties)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> postgresParallelSource = env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2);

        OutputTag<Tuple3<String, String, String[]>> partitionInfoTag = new OutputTag<Tuple3<String, String, String[]>>("partition_info") {};
        OutputTag<Tuple3<String, String, String[]>> tableInfoTag = new OutputTag<Tuple3<String, String, String[]>>("table_info") {};
        OutputTag<Tuple3<String, String, String[]>> dataCommitInfoTag = new OutputTag<Tuple3<String, String, String[]>>("data_commit_info") {};

        SingleOutputStreamOperator<Tuple3<String, String, String[]>> mainStream = postgresParallelSource
                .map(new TabelLevelAssets.PartitionDescProcessFunction.PartitionDescMapper())
                .process(new TabelLevelAssets.PartitionDescProcessFunction());

        SingleOutputStreamOperator<Tuple2<String,Integer>> partitionInfoProgress = mainStream.getSideOutput(partitionInfoTag)
                .keyBy(value -> value.f1)
                .process(new TabelLevelAssets.PartitionDescProcessFunction.PartitionInfoProcessFunction());

        SingleOutputStreamOperator<Tuple3<String,Integer,Long>> dataCommitInfoProcess = mainStream.getSideOutput(dataCommitInfoTag)
                .keyBy(value -> value.f1)
                .process(new TabelLevelAssets.PartitionDescProcessFunction.AccumulateValueProcessFunction());

        SingleOutputStreamOperator<Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>> table_level_assets = mainStream.getSideOutput(tableInfoTag)
                .keyBy(value -> value.f1)
                .connect(partitionInfoProgress.keyBy(value -> value.f0))
                .process(new TabelLevelAssets.PartitionDescProcessFunction.MergeFunction0())
                .keyBy(value -> value.f0)
                .connect(dataCommitInfoProcess.keyBy(value -> value.f0))
                .process(new TabelLevelAssets.PartitionDescProcessFunction.MergeFunction3());

        JdbcConnectionOptions build = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://localhost:5432/lakesoul_test")
                .withDriverName("org.postgresql.Driver")
                .withUsername("lakesoul_test")
                .withPassword("lakesoul_test")
                .build();
        String tableLevelAssetsSql = "INSERT INTO table_level_assets (table_id, table_name, domain, creator, namespace, partition_counts, file_counts, file_total_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (table_id) DO UPDATE SET partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size";
        String dataBaseLevelAssetsSql = "INSERT INTO namespace_level_assets (namespace, table_counts, partition_counts, file_counts, file_total_size) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON CONFLICT (namespace) DO UPDATE SET table_counts = EXCLUDED.table_counts,partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size";
        String domainLevelAssetsSql = "INSERT INTO domain_level_assets (domain, table_counts, partition_counts, file_counts, file_total_size) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON CONFLICT (domain) DO UPDATE SET table_counts = EXCLUDED.table_counts,partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size";
        String userLevelAssetsSql = "INSERT INTO user_level_assets (creator, domain_counts, namespace_counts, table_counts, partition_counts, file_counts, file_total_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (creator) DO UPDATE SET domain_counts = EXCLUDED.domain_counts,namespace_counts = EXCLUDED.namespace_counts,table_counts = EXCLUDED.table_counts,partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size";

        SinkFunction<Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>> sink = JdbcSink.sink(
                tableLevelAssetsSql,
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setString(2, t.f1);
                    ps.setString(3, t.f2);
                    ps.setString(4, t.f3);
                    ps.setString(5, t.f4);
                    ps.setInt(6, t.f6);
                    ps.setInt(7, t.f8);
                    ps.setLong(8, t.f9);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(50)
                        .withMaxRetries(0)
                        .build(),
                build
        );
        SinkFunction<Tuple5<String, Integer, Integer, Integer, Long>> namespaceAssetsSink = JdbcSink.sink(
                dataBaseLevelAssetsSql,
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setInt(2, t.f1);
                    ps.setInt(3, t.f2);
                    ps.setInt(4, t.f3);
                    ps.setLong(5, t.f4);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(50)
                        .withMaxRetries(0)
                        .build(),
                build

        );

        SinkFunction<Tuple5<String, Integer, Integer, Integer, Long>> domainAssetsSink = JdbcSink.sink(
                domainLevelAssetsSql,
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setInt(2, t.f1);
                    ps.setInt(3, t.f2);
                    ps.setInt(4, t.f3);
                    ps.setLong(5, t.f4);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(50)
                        .withMaxRetries(0)
                        .build(),
                build
        );

        SinkFunction<Tuple7<String,Integer,Integer, Integer, Integer, Integer, Long>> userAssetsSink = JdbcSink.sink(
                userLevelAssetsSql,
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setInt(2, t.f1);
                    ps.setInt(3, t.f2);
                    ps.setInt(4, t.f3);
                    ps.setInt(5,t.f4);
                    ps.setInt(6,t.f5);
                    ps.setLong(7, t.f6);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(50)
                        .withMaxRetries(0)
                        .build(),
                build
        );

        SingleOutputStreamOperator<Tuple5<String, Integer, Integer, Integer, Long>> dataBaseLevleAssets = table_level_assets.keyBy(value -> value.f2)
                .process(new DataBaseLevelAssets.PartitionInfoProcessFunction());
        SingleOutputStreamOperator<Tuple5<String, Integer, Integer, Integer, Long>> domainLevelAssets = table_level_assets.keyBy(value -> value.f3)
                .process(new DomainLevelAssets.PartitionInfoProcessFunction());
        SingleOutputStreamOperator<Tuple7<String, Integer, Integer, Integer, Integer, Integer, Long>> userLevelAssets = table_level_assets.keyBy(value -> value.f4)
                .process(new UserLevelAssets.PartitionInfoProcessFunction());

        table_level_assets.addSink(sink);
        domainLevelAssets.addSink(domainAssetsSink);
        userLevelAssets.addSink(userAssetsSink);
        dataBaseLevleAssets.addSink(namespaceAssetsSink);

        env.execute("Output Postgres Snapshot and Count Distinct Partition Desc");
    }
}