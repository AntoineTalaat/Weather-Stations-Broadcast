public interface IBitcask {
    public void open(String directoryName);
    public byte[] get(byte[] key);
    public void put(byte[] key, byte[] value);
    public byte[][] list_keys();
    public void merge(String directoryName);
}
