package com.lakesoul.Asstes;

public class TableCount {
    String tableId;
    int partitionsCount;
    int fileCount;
    long fileTotalSize;

    public TableCount(String tableId, int partitionScount, int fileCount, long fileTotalSize) {
        this.tableId = tableId;
        this.partitionsCount = partitionScount;
        this.fileCount = fileCount;
        this.fileTotalSize = fileTotalSize;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public int getPartitionScount() {
        return partitionsCount;
    }

    public void setPartitionScount(int partitionScount) {
        this.partitionsCount = partitionScount;
    }

    public int getFileCount() {
        return fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public long getFileTotalSize() {
        return fileTotalSize;
    }

    public void setFileTotalSize(long fileTotalSize) {
        this.fileTotalSize = fileTotalSize;
    }
}
