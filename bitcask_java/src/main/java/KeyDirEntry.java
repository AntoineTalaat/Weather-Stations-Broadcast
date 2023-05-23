/**
 * This Class is for the entry of KeyDir
 * Entries are in that form
 * key -> file_id | value_sz | value_pos | timestamp (from bitcask)
 */

public class KeyDirEntry {
    private String fileID;
    private int valueSz;    // length of all the appended data
    private long valuePos;  // offset before writing the appended data
    private long timestamp;


    public KeyDirEntry(String fileID, int valueSz, long valuePos, long timestamp) {
        this.fileID = fileID;
        this.valueSz = valueSz;
        this.valuePos = valuePos;
        this.timestamp = timestamp;
    }

    public String getFileID() {
        return fileID;
    }

    public int getValueSz() {
        return valueSz;
    }

    public long getValuePos() {
        return valuePos;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
