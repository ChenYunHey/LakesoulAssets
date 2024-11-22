package com.lakesoul.Asstes;

public class NameSpaceCount {
    String nameSpace;
    String domain;
    int tableCounts;
    int fileCounts;
    int partitionCounts;
    long fileTotalSize;

    public NameSpaceCount(String nameSpace, String domain, int tableCounts, int fileCounts, int partitionCounts, long fileTotalSize) {
        this.nameSpace = nameSpace;
        this.domain = domain;
        this.tableCounts = tableCounts;
        this.fileCounts = fileCounts;
        this.partitionCounts = partitionCounts;
        this.fileTotalSize = fileTotalSize;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getTableCounts() {
        return tableCounts;
    }

    public void setTableCounts(int tableCounts) {
        this.tableCounts = tableCounts;
    }

    public int getFileCounts() {
        return fileCounts;
    }

    public void setFileCounts(int fileCounts) {
        this.fileCounts = fileCounts;
    }

    public int getPartitionCounts() {
        return partitionCounts;
    }

    public void setPartitionCounts(int partitionCounts) {
        this.partitionCounts = partitionCounts;
    }

    public long getFileTotalSize() {
        return fileTotalSize;
    }

    public void setFileTotalSize(long fileTotalSize) {
        this.fileTotalSize = fileTotalSize;
    }
}
