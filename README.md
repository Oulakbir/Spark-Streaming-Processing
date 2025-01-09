Spark Streaming Processing

---
# Spark envirment Preparation

## Lounch your spark cluster using the command docker compose:
```docker
docker-compose up -d
```
Here is the content of the dokcer-compose file: 
```docker
version: '3' 
services: 
  namenode: 
    image: apache/hadoop:3.3.6 
    hostname: namenode 
    command: ["hdfs", "namenode"] 
    ports: 
      - 9870:9870 
      - 8020:8020 
    env_file: 
      - ./config 
    environment: 
      ENSURE_NAMENODE_DIR: "/tmp/hadoop-root/dfs/name" 
    volumes: 
      - ./volumes/namenode:/data 
    networks: 
      - spark-network 
 
  datanode: 
    image: apache/hadoop:3.3.6 
    command: ["hdfs", "datanode"] 
    env_file: 
      - ./config 
    networks: 
      - spark-network 
 
  resourcemanager: 
    image: apache/hadoop:3.3.6 
    hostname: resourcemanager 
    command: ["yarn", "resourcemanager"] 
    ports: 
      - 8088:8088 
    env_file: 
      - ./config 
    volumes: 
      - ./test.sh:/opt/test.sh 
    networks: 
      - spark-network 
 
  nodemanager: 
    image: apache/hadoop:3.3.6 
    command: ["yarn", "nodemanager"] 
    env_file: 
      - ./config 
    networks: 
      - spark-network 
 
  spark-master: 
    image: bitnami/spark:latest 
    container_name: spark-master 
    environment: 
      - SPARK_MODE=master 
      - SPARK_MASTER_PORT=7077 
      - SPARK_MASTER_WEBUI_PORT=8080 
4 
      - SPARK_DAEMON_MEMORY=1g 
    ports: 
      - "7077:7077" 
      - "8082:8080"  # Change the host port from 8081 to 8082 
    volumes: 
      - ./volumes/spark-master:/bitnami 
    networks: 
      - spark-network 
 
  spark-worker-1: 
    image: bitnami/spark:latest 
    container_name: spark-worker-1 
    environment: 
      - SPARK_MODE=worker 
      - SPARK_MASTER_URL=spark://spark-master:7077 
      - SPARK_WORKER_MEMORY=1g 
      - SPARK_WORKER_WEBUI_PORT=4040 
    depends_on: 
      - spark-master 
    volumes: 
      - ./volumes/spark-worker-1:/bitnami 
    ports: 
      - "4040:4040" 
    networks: 
      - spark-network 
 
  spark-worker-2: 
    image: bitnami/spark:latest 
    container_name: spark-worker-2 
    environment: 
      - SPARK_MODE=worker 
      - SPARK_MASTER_URL=spark://spark-master:7077 
      - SPARK_WORKER_MEMORY=1g 
      - SPARK_WORKER_WEBUI_PORT=4041 
    depends_on: 
      - spark-master 
    volumes: 
      - ./volumes/spark-worker-2:/bitnami 
    ports: 
      - "4041:4041" 
    networks: 
      - spark-network 
 
networks: 
  spark-network: 
    driver: bridge 
 ```
Then you have to add the file config as well to your folder:
 ```  
CORE-SITE.XML_fs.default.name=hdfs://namenode 
CORE-SITE.XML_fs.defaultFS=hdfs://namenode 
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020 
HDFS-SITE.XML_dfs.replication=1 
MAPRED-SITE.XML_mapreduce.framework.name=yarn 
MAPRED-SITE.XML_yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=/opt/hadoop 
MAPRED-SITE.XML_mapreduce.map.env=HADOOP_MAPRED_HOME=/opt/hadoop 
MAPRED-SITE.XML_mapreduce.reduce.env=HADOOP_MAPRED_HOME=/opt/hadoop 
YARN-SITE.XML_yarn.resourcemanager.hostname=resourcemanager 
5 
YARN-SITE.XML_yarn.nodemanager.pmem-check-enabled=false 
YARN-SITE.XML_yarn.nodemanager.delete.debug-delay-sec=600 
YARN-SITE.XML_yarn.nodemanager.vmem-check-enabled=false 
YARN-SITE.XML_yarn.nodemanager.aux-services=mapreduce_shuffle 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-applications=10000 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-am-resource-percent=0.1 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.Def
 aultResourceCalculator 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.queues=default 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.capacity=100 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.user-limit-factor=1 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.maximum-capacity=100 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.state=RUNNING 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.acl_submit_applications=* 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.acl_administer_queue=* 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.node-locality-delay=40 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.queue-mappings= 
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.queue-mappings-override.enable=false
```

![Screenshot 2025-01-10 000349](https://github.com/user-attachments/assets/41571b23-e295-4ee8-a9dc-e1d1f5598715)

![Screenshot 2025-01-10 000408](https://github.com/user-attachments/assets/eab06a99-a173-4727-b096-ff8c2201d3fc)

---

# Spark Application developpment and compilation  

## Incident application :

```java
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
```
## pom.xml file:
```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>oulakbir.ilham</groupId>
    <artifactId>TP_spark_stream_processing</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <spark.version>3.5.3</spark.version>
        <scala.version>2.13</scala.version>
        <java.version>1.8</java.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.version}</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.version}</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.12</artifactId>
            <version>3.1.2</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>${java.version}</source>
                    <target>${java.version}</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```
---
## JAR file Generation :  
 
 ![image](https://github.com/user-attachments/assets/e25ac918-db4a-403f-91a8-877fc1cc7446)
 

## CSV Data Generation for streaming

![image](https://github.com/user-attachments/assets/7f09c094-a20d-4b31-a837-7cefc33f7f4f)

---
# Exécution & Results

## Execution  

![image](https://github.com/user-attachments/assets/f929c794-3597-424c-b4e3-5bfa592fd487)

## Results  

![Screenshot 2025-01-10 002544](https://github.com/user-attachments/assets/e1f6cc62-3656-410c-b8e6-4d8dfcd0d544)

![Screenshot 2025-01-10 002549](https://github.com/user-attachments/assets/f113bfa2-3b7c-41cc-847a-9170bb135214)

![Screenshot 2025-01-10 002555](https://github.com/user-attachments/assets/39f5b2d8-12eb-45e4-a2f7-bdad0dffc84b)

![Screenshot 2025-01-10 002559](https://github.com/user-attachments/assets/22fb68ac-b015-466b-9112-47538d35a20b)



