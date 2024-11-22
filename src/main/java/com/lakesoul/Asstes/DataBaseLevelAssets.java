package com.lakesoul.Asstes;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DataBaseLevelAssets {
    public static class PartitionInfoProcessFunction extends KeyedProcessFunction<String, Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long>, Tuple5< String,Integer,Integer,Integer,Long>> {

        private MapState<String, TableCount> tableState;
        private transient ValueState<TableAssets> databaseState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, TableCount> tableStateDescriptor =
                    new MapStateDescriptor<>("tableState",
                            String.class,
                            TableCount.class);


            ValueStateDescriptor<TableAssets> databaseStateDescriptor =
                    new ValueStateDescriptor<TableAssets>("databaseState", TableAssets.class);

            tableState = getRuntimeContext().getMapState(tableStateDescriptor);
            databaseState = getRuntimeContext().getState(databaseStateDescriptor);

        }

        @Override
        public void processElement(Tuple10<String, String, String, String, String, String, Integer, String, Integer, Long> input, Context ctx, Collector<Tuple5<String,Integer,Integer,Integer,Long>> out) throws Exception {
            String tableId = input.f0;
            String namespace = input.f2;
            String doamin = input.f3;
            String tabeOps = input.f5;
            String creator = input.f4;
            int partitionCount = input.f6;
            int fileCounts = input.f8;
            long fileTotalSize = input.f9;
            //获取当前database的统计信息
            TableAssets currentTableAssets = databaseState.value();
            int currentTableCount;
            int currentPartionCounts;
            int currentFileCounts;
            long currentFilesTotalSize;
            if (currentTableAssets == null) {
                currentTableCount = 0;
                currentPartionCounts = 0;
                currentFileCounts = 0;
                currentFilesTotalSize = 0;
            } else {
                currentTableCount = currentTableAssets.tableCounts;
                currentPartionCounts = currentTableAssets.partitionCounts;
                currentFileCounts = currentTableAssets.fileCounts;
                currentFilesTotalSize = currentTableAssets.fileTotalSize;
            }
            if (!tabeOps.equals("delete")){
                if (!tableState.contains(tableId)){
                    TableAssets tableAssets = new TableAssets(namespace,creator,doamin,currentTableCount+1,currentPartionCounts+partitionCount,currentFileCounts+fileCounts,currentFilesTotalSize+fileTotalSize);
                    databaseState.update(tableAssets);
                } else {
                    int oldFileCount = tableState.get(tableId).fileCount;
                    int oldPartitionsCount = tableState.get(tableId).partitionsCount;
                    long oldFileTotalSize = tableState.get(tableId).fileTotalSize;
                    TableAssets tableAssets = new TableAssets(namespace,creator,doamin,currentTableCount,currentPartionCounts+partitionCount-oldPartitionsCount,currentFileCounts+fileCounts-oldFileCount,currentFilesTotalSize+fileTotalSize-oldFileTotalSize);
                    databaseState.update(tableAssets);
                }
                TableCount tableCount = new TableCount(tableId,partitionCount,fileCounts,fileTotalSize);
                tableState.put(tableId,tableCount);
                out.collect(new Tuple5<>(namespace,databaseState.value().tableCounts,databaseState.value().partitionCounts,databaseState.value().fileCounts,databaseState.value().fileTotalSize));
            } else {
                if (tableState.contains(tableId)){
                    TableAssets tableAssets = new TableAssets(namespace,creator,doamin,currentTableCount-1,currentPartionCounts-partitionCount,currentFileCounts-fileCounts,currentFilesTotalSize-fileTotalSize);
                    databaseState.update(tableAssets);
                    tableState.remove(tableId);
                    out.collect(new Tuple5<>(namespace,databaseState.value().tableCounts,databaseState.value().partitionCounts,databaseState.value().fileCounts,databaseState.value().fileTotalSize));
                }
            }
        }
    }

}
