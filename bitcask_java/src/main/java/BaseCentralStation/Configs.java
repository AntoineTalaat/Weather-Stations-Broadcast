package BaseCentralStation;

import java.util.ArrayList;

public class Configs {
    public static final String server = "127.0.0.1:9092" ;
    public static final String topic = "test-topic" ;
    public static int consumerResponseTime = 0 ;
    public static int consumerMessages = 0 ;
    public static final ArrayList<Long> messageLatency = new ArrayList<>();
    public static int messages = 1000;
    public static final int PARQUET_LIMIT = 10000;
    public static final String AVRO_SCHEMA_PATH = "src/main/java/BaseCentralStation/weatherStationAvro.avsc";
    public static final String AVRO_W_SCHEMA_PATH = "src/main/java/BaseCentralStation/weatherOnly.avsc";

}
