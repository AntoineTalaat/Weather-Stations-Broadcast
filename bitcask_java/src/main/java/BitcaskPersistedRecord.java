import java.nio.ByteBuffer;
import java.util.Arrays;

public class BitcaskPersistedRecord {
    private long timestamp;
    private byte[] key;
    private byte[] value;

    BitcaskPersistedRecord(long timestamp, byte[] keyBytes, byte[] valueBytes){
        this.timestamp = timestamp;
        this.key = keyBytes;
        this.value = valueBytes;
    }

    BitcaskPersistedRecord(byte[] record){
        // Read the timestamp (first 8 bytes) as a long
        // Read the key size (next 4 bytes) as an integer // TODO change keysize to a short
        // Read the value size (next 4 bytes) as an integer
        // Read the key (next keySize bytes)
        // Read the value (next valueSize bytes)

        this.timestamp = ByteBuffer.wrap(Arrays.copyOfRange(record, 0, 8)).getLong();
        int keySize = ByteBuffer.wrap(Arrays.copyOfRange(record, 8, 12)).getInt();
        int valueSize = ByteBuffer.wrap(Arrays.copyOfRange(record, 12, 16)).getInt();
        this.key = Arrays.copyOfRange(record, 16, 16 + keySize);
        this.value= Arrays.copyOfRange(record, 16 + keySize, 16 + keySize + valueSize);
    }

    /**
     *
     * @return record to be persisted on disk that has the following format
     *          tstamp | keysize | valuesize | key | value
     */
    public byte[] getRecordBytes(){
        byte[] timestampBytes = ByteBuffer.allocate(Long.BYTES).putLong(this.timestamp).array();
        byte[] keySizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(this.key.length).array();
        byte[] valueSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(this.value.length).array();
        // we have key bytes
        // we have value bytes

        byte[] result = null;
        result = this.concatenateByteArrays(timestampBytes,keySizeBytes);
        result = this.concatenateByteArrays(result,valueSizeBytes);
        result = this.concatenateByteArrays(result,this.key);
        result = this.concatenateByteArrays(result,this.value);

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

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
