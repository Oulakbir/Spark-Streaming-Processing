package oulakbir.ilham;

import org.apache.spark.sql .*;

import static org.apache.spark.sql.functions.col;

public class App1 {
    public static void main(String[] args) throws AnalysisException {
        SparkSession ss = SparkSession.builder()
                .appName("TP 1 SPARK SQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df1 = ss.read().option("header","true").option("inferSchema","true").csv("products.csv");
        df1.printSchema();
        df1.createTempView("products");
        //df1.show();
        //1- les produits avec prix>=20000
        //df1.where("price>=20000").show();
        //df1.where(col("price").gt(20000)).show();
        //2- le prix moyen par marque
        //df1.groupBy(col("Name")).avg("Price").show();
        //3- afficher les produits par ordre decroissant de leur prix
         df1.select(col("Name"),col("Price")).orderBy(col("Price").desc()).show();
        //traitement en utilisant SQL
        //ss.sql("select * from products where price > 20000").show();
        //ss.sql("select Name, avg(Price) from products group by Name").show();
    }
}