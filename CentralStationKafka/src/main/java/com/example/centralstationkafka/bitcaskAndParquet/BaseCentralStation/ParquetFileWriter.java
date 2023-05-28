package com.example.centralstationkafka.bitcaskAndParquet.BaseCentralStation;

import com.example.centralstationkafka.bitcaskAndParquet.BaseCentralStation.StationMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Stat;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class ParquetFileWriter {
    private static List<StationMessage> messages;
    private static Schema schema;
    private static final String baseFile = "/hello/archive";
    static {
        messages = new LinkedList<>();
        try {
            schema = new Schema.Parser().parse(new File(Configs.AVRO_SCHEMA_PATH));
            checkDirectoryExistOrCreate(baseFile);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void receive(StationMessage message){
        messages.add(message);
        if(messages.size()>= Configs.PARQUET_LIMIT){
            // output to file
            // empty the list
            writeToParquet(messages);
            messages= new LinkedList<>();
        }
    }


    private void writeToParquet(List<StationMessage> messages){
            Thread thread = new Thread(() -> {
                Hashtable<String,List<StationMessage>> map = new Hashtable<>();
                for(StationMessage message:messages){ // iterate the 10k messages
                    long unixTimestamp = message.status_timestamp;
//                    LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), ZoneId.systemDefault());
                    Date date = new Date(unixTimestamp);

                    // Format the date to extract year, month, and day
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    String formattedDate = dateFormat.format(date);
                    String dateStr = baseFile + "\\" + formattedDate ;
                    boolean dayDirectoryExists = false;
                    try {dayDirectoryExists = checkDirectoryExistOrCreate(dateStr);} catch (FileSystemException e) {e.printStackTrace();}
                    if(!dayDirectoryExists)
                        for(int i=1;i<10;i++)
                            map.put(dateStr+"/"+i,new ArrayList<>()); // create the stations empty entries

                    String stationIDStr = dateStr + "/" + message.station_id;
                    map.get(stationIDStr).add(message);
                } // at this point I'm supposed to have divided the batches
                // see comment at the end

                map.forEach((key, list) -> {
                    // key is the directory file, value is a list of messages
                    // we need a name for the file
                    // to ensure the files are ordered in a reasonable way, we will use timestamp

                    if(!list.isEmpty()){
                        long placementTimestamp = System.currentTimeMillis();
                        String newParquetFileName = key + "/" + placementTimestamp + ".parquet";
                        // write
                        try {
                            System.out.println(list.get(0).station_id +"-" + list.get(0).status_timestamp );
                            writeToParquetAux(newParquetFileName,list);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                });



            });
        thread.start();

    }





    private void writeToParquetAux(String newParquetFileName, List<StationMessage> list) throws IOException {
            ParquetWriter<Object> writer =  AvroParquetWriter.builder(new Path(newParquetFileName))
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withSchema(schema)
                    .build();
        System.out.println("HERE >>>> "+list.size());
                for(StationMessage m : list){
                    writer.write(m.getGenericRecord(schema));
                    System.out.println("Written");
                }
                writer.close();

    }



    // I get IllegalAccessError
    private void writeToParquetAuxSpark(String newParquetFileName, List<StationMessage> list){


        String sparkMasterUrl = "local";
        SparkSession spark = SparkSession.builder()
                .appName("ParquetWriter")
                .master(sparkMasterUrl)
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.executor.memory", "1g")
                .getOrCreate();
        // Create a list of StationMessage objects
        List<StationMessage> stationMessages = new ArrayList<>();
        // Add StationMessage instances to the list

        // Define the schema for StationMessage and Weather
        StructType schema = new StructType()
                .add("station_id", DataTypes.LongType)
                .add("s_no", DataTypes.LongType)
                .add("status_timestamp", DataTypes.LongType)
                .add("battery_status", DataTypes.StringType)
                .add("weather", new StructType()
                        .add("humidity", DataTypes.IntegerType)
                        .add("temperature", DataTypes.IntegerType)
                        .add("wind_speed", DataTypes.IntegerType))
                ;

        // Create a DataFrame from the list of StationMessage objects
        Dataset<Row> df = spark.createDataFrame(stationMessages, StationMessage.class);

        // Write the DataFrame to a Parquet file
        df.write()
                .mode(SaveMode.Overwrite)
                .parquet(newParquetFileName);

        // Stop the SparkSession
        spark.stop();
    }

    private void writeToParquetAuxHadoop(String newParquetFileName, List<StationMessage> list) throws IOException {
//        String schemaPath = "/path/to/parquet/schema/file";
//        Schema schema = new Schema.Parser().parse(new File(schemaPath));
//
//        // Set up Parquet writer configuration
//        Configuration conf = new Configuration();
//        Path outputPath = new Path("/path/to/output/parquet/file");
//        CompressionCodecName compressionCodec = CompressionCodecName.SNAPPY;
//        ParquetWriter.Builder builder = ParquetWriter.builder(outputPath)
//                .withCompressionCodec(compressionCodec)
//                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
//                .withConf(conf);
//        SimpleGroupWriterBuilder builder = new SimpleGroupWriterBuilder();
//
//        // Convert Avro schema to Parquet schema and set it in the builder
//        org.apache.parquet.schema.MessageType parquetSchema =
//                MessageTypeParser.parseMessageType(schema.toString());
//        builder.withType(parquetSchema);
//
//        // Write data to Parquet file using the builder
//        ParquetWriter writer = builder.build();
//        writer.write(/* your data */);
//        writer.close();
//
    }

    private static boolean checkDirectoryExistOrCreate(String directoryPath) throws FileSystemException {
        File directory = new File(directoryPath);
        // Check if the directory exists
        if (!directory.exists()) {
            // Attempt to create the directory
            boolean created = directory.mkdirs();
            if (!created)
                throw new FileSystemException("Folder not found, Failed to create folder at: " + directoryPath);
            return false;
        }
        return true;
    }

       /*
    An Example of the directory outline
              relative        parent
              or current    directory
              directory      for parq.                station
              not sure        files      date           ID
                 |             |          |             |
                \/            \/         \/            \/
        currentDirectory / parquet / 2023-DECEMBER-12 / 5
     */
}
