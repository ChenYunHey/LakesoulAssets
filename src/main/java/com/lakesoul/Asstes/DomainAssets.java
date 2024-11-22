package com.lakesoul.Asstes;

public class DomainAssets {
    String domain;
    int namespaceCounts;
    int tableCounts;
    int partitionCounts;
    int fileCounts;
    long fileTotalSize;

    public DomainAssets(String domain, int namespaceCounts, int tableCounts, int partitionCounts, int fileCounts, long fileTotalSize) {
        this.domain = domain;
        this.namespaceCounts = namespaceCounts;
        this.tableCounts = tableCounts;
        this.partitionCounts = partitionCounts;
        this.fileCounts = fileCounts;
        this.fileTotalSize = fileTotalSize;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getNamespaceCounts() {
        return namespaceCounts;
    }

    public void setNamespaceCounts(int namespaceCounts) {
        this.namespaceCounts = namespaceCounts;
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
}
