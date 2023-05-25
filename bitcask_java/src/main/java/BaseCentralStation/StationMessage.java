package BaseCentralStation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;

public class StationMessage {
    long station_id;
    long s_no;
    long status_timestamp;
    String battery_status;
    Weather weather;



    static class Weather{
        int humidity;
        int temperature;
        int wind_speed;
    }

    public GenericData.Record getGenericRecord(Schema schema) throws IOException {
        Schema miniSchema = new Schema.Parser().parse(new File(Configs.AVRO_W_SCHEMA_PATH));

        GenericData.Record g = new GenericData.Record(schema);
        g.put("station_id",station_id);
        g.put("s_no",s_no);
        g.put("status_timestamp",status_timestamp);
        g.put("battery_status",battery_status);
        GenericData.Record w = new GenericData.Record(miniSchema);
            w.put("humidity", this.weather.humidity);
            w.put("temperature",weather.temperature);
            w.put("wind_speed",weather.wind_speed);
        g.put("weather",w);

        return g;
    }

}

