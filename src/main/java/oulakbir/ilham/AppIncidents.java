package oulakbir.ilham;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
public class AppIncidents {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // Désactiver les logs inutiles
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        // Créer une session Spark
        SparkSession ss = SparkSession.builder()
                .appName("Spark Structured Streaming - Incidents")
                .getOrCreate();

        // Définir le schéma des fichiers CSV
        StructType schema = new StructType(new StructField[]{
                new StructField("Id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("titre", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
                new StructField("service", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date", DataTypes.StringType, true, Metadata.empty())
        });
        // Lire les données en streaming
        Dataset<Row> inputTable = ss.readStream()
                .schema(schema)
                .option("header", "true")
                .csv("hdfs://namenode:8020/input-incidents");
        // 1. Afficher le nombre d'incidents par service
        Dataset<Row> incidentsByService = inputTable.groupBy("service").count();
        // 2. Extraire l'année de la colonne "date" et calculer le nombre d'incidents par année
        Dataset<Row> incidentsByYear = inputTable.withColumn("year", inputTable.col("date").substr(0, 4))
                .groupBy("year")
                .count()
                .orderBy(org.apache.spark.sql.functions.desc("count"))
                .limit(2); // Prendre seulement les deux années avec le plus d'incidents
        // Démarrer les requêtes de streaming
        StreamingQuery query1 = incidentsByService.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        StreamingQuery query2 = incidentsByYear.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        // Attendre la fin des requêtes
        query1.awaitTermination();
        query2.awaitTermination();
    }
}
