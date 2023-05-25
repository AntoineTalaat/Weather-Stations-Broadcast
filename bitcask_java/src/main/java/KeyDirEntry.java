/**
 * This Class is for the entry of KeyDir
 * Entries are in that form
 * key -> file_id | value_sz | value_pos | timestamp (from bitcask)
 */

public class KeyDirEntry {
    private String fileID;
    private final int valueSz;    // length of all the appended data
    private final int valuePos;  // offset before writing the appended data
    private final long timestamp;


    public KeyDirEntry(String fileID, int valueSz, int valuePos, long timestamp) {
        this.fileID = fileID;
        this.valueSz = valueSz;
        this.valuePos = valuePos;
        this.timestamp = timestamp;
    }

    public String getFileID() {
        return fileID;
    }
    public void setFileID(String newFileID){
        this.fileID = newFileID ;
    }

    public int getValueSz() {
        return valueSz;
    }

    public int getValuePos() {
        return valuePos;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
