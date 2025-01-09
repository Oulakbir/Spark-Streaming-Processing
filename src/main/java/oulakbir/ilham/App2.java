package oulakbir.ilham;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;



public class App2 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession ss = SparkSession.builder()
                .appName("SPark Streaming")
                .getOrCreate();
        StructType schema = new StructType(new StructField[]{
                new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Quantity", DataTypes.StringType, true, Metadata.empty()),
        });
        Dataset<Row> inputTable=ss.readStream().schema(schema).option("header","true").csv("hdfs://namenode:8020/input");
        Dataset<Row> outPutTable=inputTable.groupBy("Name").count();
        StreamingQuery query= outPutTable.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime(5000))
                .start();
        query.awaitTermination();
    }
}
