package com.webdev;

public class TempKeyDirEntry {
    public final int fileId;
    public final int oldFileId;
    public final long valueOffset;
    public final int valueSize;

    public TempKeyDirEntry(int fileId, int oldFileId, long valueOffset, int valueSize) {
        this.fileId = fileId;
        this.valueOffset = valueOffset;
        this.valueSize = valueSize;
        this.oldFileId = oldFileId;
    }
}
