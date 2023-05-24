import java.nio.ByteBuffer;

public class BitcaskPersistedRecord {
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;

    BitcaskPersistedRecord(long timestamp, byte[] keyBytes, byte[] valueBytes) {
        this.timestamp = timestamp;
        this.key = keyBytes;
        this.value = valueBytes;
    }

    /**
     * @return record to be persisted on disk that has the following format
     * tstamp | keysize | valuesize | key | value
     */
    public byte[] getRecordBytes() {
        byte[] timestampBytes = ByteBuffer.allocate(Constants.TimeStampSize).putLong(this.timestamp).array();
        byte[] keySizeBytes = ByteBuffer.allocate(Constants.KeySize).putInt(this.key.length).array();
        byte[] valueSizeBytes = ByteBuffer.allocate(Constants.ValueSize).putInt(this.value.length).array();
        // we have key bytes
        // we have value bytes

        byte[] result ;
        result = this.concatenateByteArrays(timestampBytes, keySizeBytes);
        result = this.concatenateByteArrays(result, valueSizeBytes);
        result = this.concatenateByteArrays(result, this.key);
        result = this.concatenateByteArrays(result, this.value);

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
