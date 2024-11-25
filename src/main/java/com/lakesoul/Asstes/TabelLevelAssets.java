package com.lakesoul.Asstes;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class TabelLevelAssets {
    public static class PartitionDescProcessFunction extends ProcessFunction<Tuple3<String, String, String[]>, Tuple3<String, String, String[]>> {

        @Override
        public void processElement(Tuple3<String, String, String[]> value, Context ctx, Collector<Tuple3<String, String, String[]>> out) throws Exception {
            // 根据表名选择旁数据流
            switch (value.f0) {
                case "partition_info":
                    ctx.output(new OutputTag<Tuple3<String, String, String[]>>("partition_info") {
                    }, value);
                    break;
                case "table_info":
                    ctx.output(new OutputTag<Tuple3<String, String, String[]>>("table_info") {
                    }, value);
                    break;
                case "data_commit_info":
                    ctx.output(new OutputTag<Tuple3<String, String, String[]>>("data_commit_info") {
                    }, value);
                    break;
            }
        }

        public static class PartitionDescMapper implements MapFunction<String, Tuple3<String, String, String[]>> {

            @Override
            public Tuple3<String, String, String[]> map(String s) {
                // 解析传入的 JSON 字符串
                JSONObject parse = (JSONObject) JSONObject.parse(s);
                String PGtableName = parse.get("tableName").toString();
                String[] tableInfos = new String[5];
                JSONObject afterJson = null;
                JSONObject commitJson = null;

                // 处理 partition_info 表
                if (PGtableName.equals("partition_info")) {
                    if (parse.getJSONObject("after").size() > 0){
                        commitJson = (JSONObject) parse.get("after");
                    } else {
                        commitJson = (JSONObject) parse.get("before");
                    }
                    String partitionDesc = commitJson.getString("partition_desc");
                    String pgTableID = commitJson.getString("table_id");
                    String commitOp = parse.getString("commitOp");
                    tableInfos[0] = partitionDesc;
                    tableInfos[1] = commitOp;
                    return new Tuple3<>(PGtableName, pgTableID, tableInfos);  // 返回包含 tableName, tableID 和 partitionDesc 的元组
                }

                // 处理 table_info 表
                if (PGtableName.equals("table_info")) {
                    if (parse.getJSONObject("after").size() > 0){
                        commitJson = (JSONObject) parse.get("after");
                    } else {
                        commitJson = (JSONObject) parse.get("before");
                    }
                    String tableId = commitJson.getString("table_id");
                    String tableNamespace = commitJson.getString("table_namespace");
                    String tableName = commitJson.getString("table_name");
                    String domain = commitJson.getString("domain");
                    String creator = commitJson.getString("creator");
                    String commitOp = parse.getString("commitOp");
                    tableInfos[0] = tableNamespace;
                    tableInfos[1] = tableName;
                    tableInfos[2] = domain;
                    tableInfos[3] = creator;
                    tableInfos[4] = commitOp;
                    return new Tuple3<>(PGtableName, tableId, tableInfos);
                }

                // 处理 data_commit_info 表
                if (PGtableName.equals("data_commit_info")) {
                    if (parse.getJSONObject("after").size() > 0){
                        commitJson = (JSONObject) parse.get("after");
                    } else {
                        commitJson = (JSONObject) parse.get("before");
                    }

                    String tableId = commitJson.getString("table_id");
                    JSONArray fileOps = commitJson.getJSONArray("file_ops");
                    String committed = commitJson.getString("committed");
                    String commitOp = parse.getString("commitOp");
                    long fileBytesSize = 0L;
                    int fileCount = 0;
                    for (Object op : fileOps) {
                        //JSONObject jop = (JSONObject) op;
                        //Object arry = jop.get("array");
                        //String binary = arry.toString();
                        byte[] decode = Base64.getDecoder().decode(op.toString());
                        String fileOp = new String(decode, StandardCharsets.UTF_8);
                        AssetsUtils assetsUtils = new AssetsUtils();
                        String[] fileOpsString = assetsUtils.parseFileOpsString(fileOp);

                        if (fileOpsString[0].equals("add")){
                            fileCount ++;
                            fileBytesSize = fileBytesSize + Long.parseLong(fileOpsString[1]);
                        }
                    }

                    tableInfos[0] = String.valueOf(fileCount);
                    tableInfos[1] = String.valueOf(fileBytesSize);
                    tableInfos[2] = committed;
                    tableInfos[3] = commitOp;
                    return new Tuple3<>(PGtableName, tableId, tableInfos);
                }
                return null;
            }
        }

        public static class AccumulateValueProcessFunction extends ProcessFunction<Tuple3<String, String, String[]>, Tuple3<String,Integer,Long>> {

            // 定义一个 ValueState 来存储累加的值
            private transient ValueState<Integer> fileCountsAccumulatedValue;
            private transient ValueState<Long> fileBytesSizeAccumulatedValue;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化累加状态
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                        "fileCountsAccumulatedValue", // 状态的名称
                        Integer.class,      // 状态的类型
                        0                   // 默认值为 0
                );
                fileCountsAccumulatedValue = getRuntimeContext().getState(descriptor);
                ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<>(
                        "FileBytesSizeAccumulatedValue", // 状态的名称
                        Long.class,          // 状态的类型
                        0L                   // 默认值为 0L
                );
                fileBytesSizeAccumulatedValue = getRuntimeContext().getState(descriptor2);

            }

            @Override
            public void processElement(Tuple3<String, String, String[]> value, Context ctx, Collector<Tuple3<String,Integer,Long>> out) throws Exception {
                Tuple3<String, Integer, Long> res = new Tuple3<>();
                int currentValue = Integer.parseInt(value.f2[0]);
                long currentFileBytesSize = Long.parseLong(value.f2[1]);
                boolean commited = Boolean.parseBoolean(value.f2[2]);
                String commitOp = value.f2[3];
                // 获取当前状态中的累加值
                Integer previousValue = fileCountsAccumulatedValue.value();
                long fileBytesSizePreviousValue = fileBytesSizeAccumulatedValue.value();
                res.f0 = value.f1;
                if (commitOp.equals("delete")){
                    int newAccumulatedValue = previousValue - currentValue;
                    long newFileBytesize = fileBytesSizePreviousValue - currentFileBytesSize;
                    fileCountsAccumulatedValue.update(newAccumulatedValue);
                    fileBytesSizeAccumulatedValue.update(newFileBytesize);
                    res.f1 = newAccumulatedValue;
                    res.f2 = newFileBytesize;
                    out.collect(res);
                } else {
                    if (commited) {
                        // 计算新的累加值
                        int newAccumulatedValue = previousValue + currentValue;
                        long newFileBytesize = fileBytesSizePreviousValue + currentFileBytesSize;
                        // 更新累加状态
                        fileCountsAccumulatedValue.update(newAccumulatedValue);
                        fileBytesSizeAccumulatedValue.update(newFileBytesize);
                        res.f1 = newAccumulatedValue;
                        res.f2 = newFileBytesize;
                        out.collect(res);
                    }
                }
            }
        }

        public static class PartitionInfoProcessFunction extends KeyedProcessFunction<String, Tuple3<String, String, String[]>, Tuple2<String, Integer>> {

            // 用 MapState 来存储每个 table_id 对应的不同 partition_desc 的数量
            private MapState<String, Boolean> partitionDescState;
            private MapState<String, Integer> tablePartitionCountState;

            @Override
            public void open(Configuration parameters) {
                // 在 open 方法中初始化状态
                MapStateDescriptor<String, Boolean> partitionDescStateDescriptor =
                        new MapStateDescriptor<>(
                                "partitionDescState", // 状态的名称
                                String.class, // key 类型
                                Boolean.class); // value 类型，用来标识该 partition_desc 是否已经出现

                partitionDescState = getRuntimeContext().getMapState(partitionDescStateDescriptor);

                // 初始化 table_id 和对应分区数量的状态
                MapStateDescriptor<String, Integer> tablePartitionCountStateDescriptor =
                        new MapStateDescriptor<>(
                                "tablePartitionCountState", // 状态的名称
                                String.class, // key 类型
                                Integer.class); // value 类型，记录分区的数量

                tablePartitionCountState = getRuntimeContext().getMapState(tablePartitionCountStateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, String[]> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String tableId = input.f1;       // 表ID
                String partitionDesc = input.f2[0];
                String commitOp = input.f2[1];
                Tuple2<String,Integer> partitionCounts = new Tuple2<>();
                // 获取当前 table_id 的分区数量
                Integer currentCount = tablePartitionCountState.get(tableId);
                if (currentCount == null) {
                    currentCount = 0;
                }
                if (partitionDesc != null){
                    if (!commitOp.equals("delete")){
                        if (!partitionDescState.contains(partitionDesc)) {
                            partitionDescState.put(partitionDesc, true); // 标记该 partition_desc 已经出现
                            tablePartitionCountState.put(tableId, currentCount + 1); // 更新该 table_id 的分区数
                        }
                    } else {
                        if (partitionDescState.contains(partitionDesc)){
                            tablePartitionCountState.put(tableId, currentCount - 1); // 更新该 table_id 的分区数
                            partitionDescState.remove(partitionDesc);
                        }
                    }
                    partitionCounts.f0 = tableId;
                    partitionCounts.f1 = tablePartitionCountState.get(tableId);

                    // 将结果发送到下游
                    out.collect(partitionCounts);
                }
            }
        }

        public static class MergeFunction0 extends CoProcessFunction<Tuple3<String, String, String[]>, Tuple2<String, Integer>, Tuple7<String, String, String, String, String, String, Integer>> {

            private ValueState<Tuple3<String, String, String[]>> latestTableInfo;
            private ValueState<Tuple2<String, Integer>> latestPartitionInfo;

            @Override
            public void open(Configuration parameters) {
                // 为流A定义ValueStateDescriptor，存储最新的Tuple3数据
                ValueStateDescriptor<Tuple3<String, String, String[]>> tableInfoStateDesc =
                        new ValueStateDescriptor<>("latestTableInfo", TypeInformation.of(new TypeHint<Tuple3<String, String, String[]>>() {}));

                // 为流B定义ValueStateDescriptor，存储最新的Tuple2数据
                ValueStateDescriptor<Tuple2<String, Integer>> partitionInfoStateDesc =
                        new ValueStateDescriptor<>("latestPartitionInfo", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

                // 获取状态
                latestTableInfo = getRuntimeContext().getState(tableInfoStateDesc);
                latestPartitionInfo = getRuntimeContext().getState(partitionInfoStateDesc);
            }

            @Override
            public void processElement1(Tuple3<String, String, String[]> valueTableInfo,
                                        CoProcessFunction<Tuple3<String, String, String[]>, Tuple2<String, Integer>, Tuple7<String, String, String, String, String, String, Integer>>.Context context,
                                        Collector<Tuple7<String, String, String, String, String, String, Integer>> collector) throws Exception {
                // 更新最新的TableInfo
                latestTableInfo.update(valueTableInfo);

                // 获取最新的PartitionInfo
                Tuple2<String, Integer> latestPartitionInfoValue = latestPartitionInfo.value();

                // 如果两个流的数据有匹配，进行Join
                if (latestPartitionInfoValue != null && valueTableInfo.f1.equals(latestPartitionInfoValue.f0)) {
                    collector.collect(new Tuple7<>(valueTableInfo.f1, valueTableInfo.f2[1], valueTableInfo.f2[0], valueTableInfo.f2[2], valueTableInfo.f2[3], valueTableInfo.f2[4], latestPartitionInfoValue.f1));
                }
            }

            @Override
            public void processElement2(Tuple2<String, Integer> valuePartitionInfo,
                                        CoProcessFunction<Tuple3<String, String, String[]>, Tuple2<String, Integer>, Tuple7<String, String, String, String, String, String, Integer>>.Context context,
                                        Collector<Tuple7<String, String, String, String, String, String, Integer>> collector) throws Exception {
                // 更新最新的PartitionInfo
                latestPartitionInfo.update(valuePartitionInfo);

                // 获取最新的TableInfo
                Tuple3<String, String, String[]> latestTableInfoValue = latestTableInfo.value();

                // 如果两个流的数据有匹配，进行Join
                if (latestTableInfoValue != null && valuePartitionInfo.f0.equals(latestTableInfoValue.f1)) {
                    collector.collect(new Tuple7<>(latestTableInfoValue.f1, latestTableInfoValue.f2[1], latestTableInfoValue.f2[0], latestTableInfoValue.f2[2], latestTableInfoValue.f2[3], latestTableInfoValue.f2[4], valuePartitionInfo.f1));
                }
            }
        }


        public static class MergeFunction3 extends CoProcessFunction<Tuple7<String, String, String, String, String, String, Integer>, Tuple3<String, Integer, Long>, Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>> {

            private ValueState<Tuple7<String, String, String, String, String, String, Integer>> latestMainStream;
            private ValueState<Tuple3<String, Integer, Long>> latestDataCommitInfoStream;

            @Override
            public void open(Configuration parameters) {
                // 为流A定义ValueStateDescriptor，存储最新的Tuple7数据
                ValueStateDescriptor<Tuple7<String, String, String, String, String, String, Integer>> mainStreamStateDesc =
                        new ValueStateDescriptor<>("latestMainStream", TypeInformation.of(new TypeHint<Tuple7<String, String, String, String, String, String, Integer>>() {}));

                // 为流B定义ValueStateDescriptor，存储最新的Tuple3数据
                ValueStateDescriptor<Tuple3<String, Integer, Long>> dataCommitInfoStateDesc =
                        new ValueStateDescriptor<>("latestDataCommitInfoStream", TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {}));

                // 获取状态
                latestMainStream = getRuntimeContext().getState(mainStreamStateDesc);
                latestDataCommitInfoStream = getRuntimeContext().getState(dataCommitInfoStateDesc);
            }

            @Override
            public void processElement1(Tuple7<String, String, String, String, String, String, Integer> valueMain,
                                        CoProcessFunction<Tuple7<String, String, String, String, String, String, Integer>, Tuple3<String, Integer, Long>, Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>>.Context context,
                                        Collector<Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>> collector) throws Exception {
                // 更新最新的mainStream数据
                latestMainStream.update(valueMain);

                // 获取最新的commitInfo数据
                Tuple3<String, Integer, Long> latestCommitInfo = latestDataCommitInfoStream.value();

                // 如果最新数据有匹配的项，则进行 Join
                if (latestCommitInfo != null && valueMain.f0.equals(latestCommitInfo.f0)) {
                    collector.collect(new Tuple10<>(valueMain.f0, valueMain.f1, valueMain.f2, valueMain.f3, valueMain.f4, valueMain.f5, valueMain.f6, latestCommitInfo.f0, latestCommitInfo.f1, latestCommitInfo.f2));
                }
            }

            @Override
            public void processElement2(Tuple3<String, Integer, Long> valueDataCommitInfo,
                                        CoProcessFunction<Tuple7<String, String, String, String, String, String, Integer>, Tuple3<String, Integer, Long>, Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>>.Context context,
                                        Collector<Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>> collector) throws Exception {
                // 更新最新的dataCommitInfo数据
                latestDataCommitInfoStream.update(valueDataCommitInfo);
                // 获取最新的mainStream数据
                Tuple7<String, String, String, String, String, String, Integer> latestMainStreamData = latestMainStream.value();

                // 如果最新数据有匹配的项，则进行 Join
                if (latestMainStreamData != null && valueDataCommitInfo.f0.equals(latestMainStreamData.f0)) {
                    collector.collect(new Tuple10<>(latestMainStreamData.f0, latestMainStreamData.f1, latestMainStreamData.f2, latestMainStreamData.f3, latestMainStreamData.f4, latestMainStreamData.f5, latestMainStreamData.f6, valueDataCommitInfo.f0, valueDataCommitInfo.f1, valueDataCommitInfo.f2));
                }
            }
        }

    }
}
