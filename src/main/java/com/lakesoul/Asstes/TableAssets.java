package com.lakesoul.Asstes;

public class TableAssets {
    String namspace;
    String creator;
    String domain;
    int tableCounts;
    int partitionCounts;
    int fileCounts;
    long fileTotalSize;


    public TableAssets(String namspace, String creator,String domain, int tableCounts, int partitionCounts, int fileCounts, long fileTotalSize) {
        this.namspace = namspace;
        this.creator = creator;
        this.tableCounts = tableCounts;
        this.partitionCounts = partitionCounts;
        this.fileCounts = fileCounts;
        this.fileTotalSize = fileTotalSize;
        this.domain = domain;
    }

    public String getNamspace() {
        return namspace;
    }

    public void setNamspace(String namspace) {
        this.namspace = namspace;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public int getTableCounts() {
        return tableCounts;
    }

    public void setTableCounts(int tableCounts) {
        this.tableCounts = tableCounts;
    }

    public int getPartitionCounts() {
        return partitionCounts;
    }

    public void setPartitionCounts(int partitionCounts) {
        this.partitionCounts = partitionCounts;
    }

    public int getFileCounts() {
        return fileCounts;
    }

    public void setFileCounts(int fileCounts) {
        this.fileCounts = fileCounts;
    }

    public long getFileTotalSize() {
        return fileTotalSize;
    }

    public void setFileTotalSize(long fileTotalSize) {
        this.fileTotalSize = fileTotalSize;
    }

    @Override
    public String toString() {
        return "TableAssets{" +
                "namspace='" + namspace + '\'' +
                ", creator='" + creator + '\'' +
                ", tableCounts=" + tableCounts +
                ", partitionCounts=" + partitionCounts +
                ", fileCounts=" + fileCounts +
                ", fileTotalSize=" + fileTotalSize +
                '}';
    }
}
