import java.nio.ByteBuffer;


/**
 * class to store records of hint files such that they are in the format
 * tstamp |  ksz  |  valuesz  | value_pos  |  key
 */
public class BitcaskHintRecord {
    private long timestamp;
    private int pos;
    private int valueSize;
    private byte[] key;

    public BitcaskHintRecord(long timestamp,  byte[] keyBytes,int offset, int valueSize) {
        this.timestamp = timestamp;
        this.pos = offset;
        this.key = keyBytes;
        this.valueSize = valueSize ;
    }

    public byte[] getHintRecordBytes() {
        byte[] timestampBytes = ByteBuffer.allocate(Long.BYTES).putLong(this.timestamp).array();
        byte[] keySizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(this.key.length).array();
        byte[] valuePosBytes = ByteBuffer.allocate(Integer.BYTES).putLong(this.pos).array();
        byte[] valueSizeBytes = ByteBuffer.allocate(Integer.BYTES).putLong(this.valueSize).array();

        // we have key bytes
        // we have value bytes

        byte[] result = null;
        result = this.concatenateByteArrays(timestampBytes, keySizeBytes);
        result = this.concatenateByteArrays(result, this.key);
        result = this.concatenateByteArrays(result, valuePosBytes);
        result = this.concatenateByteArrays(result, valueSizeBytes);


        return result;

    }

    private byte[] concatenateByteArrays(byte[] array1, byte[] array2) {
        int length1 = array1.length;
        int length2 = array2.length;
        byte[] result = new byte[length1 + length2];
        System.arraycopy(array1, 0, result, 0, length1);
        System.arraycopy(array2, 0, result, length1, length2);
        return result;
    }
}
