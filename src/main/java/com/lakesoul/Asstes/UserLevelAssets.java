package com.lakesoul.Asstes;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UserLevelAssets {
    public static class PartitionInfoProcessFunction extends KeyedProcessFunction<String, Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>, Tuple7<String,Integer, Integer,Integer,Integer,Integer,Long>> {

        private MapState<String, TableCount> tableState;
        private transient ValueState<TableAssets> userState;
        private transient MapState<String, Boolean> namespaceCountState;
        private transient MapState<String, Boolean> domainCountState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, TableCount> tableStateDescriptor =
                    new MapStateDescriptor<>("tableState",
                            String.class,
                            TableCount.class);

            ValueStateDescriptor<TableAssets> databaseStateDescriptor =
                    new ValueStateDescriptor<TableAssets>("databaseState", TableAssets.class);

            MapStateDescriptor<String, Boolean> namespaceCountStateDescriptor =
                    new MapStateDescriptor<String, Boolean>("namespaceCountState",String.class,Boolean.class);

            MapStateDescriptor<String, Boolean> domainCountStateDescriptor =
                    new MapStateDescriptor<String, Boolean>("domainCountState", String.class, Boolean.class);

            tableState = getRuntimeContext().getMapState(tableStateDescriptor);
            userState = getRuntimeContext().getState(databaseStateDescriptor);
            namespaceCountState = getRuntimeContext().getMapState(namespaceCountStateDescriptor);
            domainCountState = getRuntimeContext().getMapState(domainCountStateDescriptor);

        }

        @Override
        public void processElement(Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long> input, Context ctx, Collector<Tuple7<String,Integer, Integer,Integer,Integer,Integer,Long>> out) throws Exception {
            String tableId = input.f0;
            String namespace = input.f2;
            String doamin = input.f3;
            String tabeOps = input.f5;
            String creator = input.f4;
            int partitionCount = input.f6;
            int fileCounts = input.f8;
            long fileTotalSize = input.f9;
            //获取当前user的统计信息
            TableAssets currentTableAssets = userState.value();
            int currentNamspaceCounts = 0;
            int currentDomainCounts = 0;
            int currentTableCounts;
            int currentPartionCounts;
            int currentFileCounts;
            long currentFilesTotalSize;
            namespaceCountState.put(namespace,true);
            domainCountState.put(doamin,true);
            for (String key : namespaceCountState.keys()) {
                currentNamspaceCounts ++ ;
            }
            for (String key : domainCountState.keys()) {
                currentDomainCounts ++ ;
            }
            if (currentTableAssets == null) {
                currentTableCounts = 0;
                currentPartionCounts = 0;
                currentFileCounts = 0;
                currentFilesTotalSize = 0;
            } else {
                currentTableCounts = currentTableAssets.tableCounts;
                currentPartionCounts = currentTableAssets.partitionCounts;
                currentFileCounts = currentTableAssets.fileCounts;
                currentFilesTotalSize = currentTableAssets.fileTotalSize;
            }
            if (!tabeOps.equals("delete")){
                if (!tableState.contains(tableId)){
                    TableAssets tableAssets = new TableAssets(namespace,creator,doamin,currentTableCounts+1,currentPartionCounts+partitionCount,currentFileCounts+fileCounts,currentFilesTotalSize+fileTotalSize);
                    userState.update(tableAssets);
                } else {
                    int oldFileCount = tableState.get(tableId).fileCount;
                    int oldPartitionsCount = tableState.get(tableId).partitionsCount;
                    long oldFileTotalSize = tableState.get(tableId).fileTotalSize;
                    TableAssets tableAssets = new TableAssets(namespace,creator,doamin,currentTableCounts,currentPartionCounts+partitionCount-oldPartitionsCount,currentFileCounts+fileCounts-oldFileCount,currentFilesTotalSize+fileTotalSize-oldFileTotalSize);
                    userState.update(tableAssets);
                }
                TableCount tableCount = new TableCount(tableId,partitionCount,fileCounts,fileTotalSize);
                tableState.put(tableId,tableCount);
                out.collect(new Tuple7<>(creator,currentDomainCounts,currentNamspaceCounts,userState.value().tableCounts,userState.value().partitionCounts,userState.value().fileCounts,userState.value().fileTotalSize));
            } else {
                if (tableState.contains(tableId)){
                    TableAssets tableAssets = new TableAssets(namespace,creator,doamin,currentTableCounts-1,currentPartionCounts-partitionCount,currentFileCounts-fileCounts,currentFilesTotalSize-fileTotalSize);
                    userState.update(tableAssets);
                    tableState.remove(tableId);
                    out.collect(new Tuple7<>(creator,currentDomainCounts,currentNamspaceCounts,userState.value().tableCounts,userState.value().partitionCounts,userState.value().fileCounts,userState.value().fileTotalSize));
                }
            }
        }
    }
}
