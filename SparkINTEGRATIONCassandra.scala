package com.bigdata.spark.Git
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._


object SparkINTEGRATIONCassandra {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("SparkINTEGRATIONCassandra").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("SparkINTEGRATIONCassandra").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val host = "jdbc:oracle:thin:@//oracledb.conbyj3qndaj.ap-south-1.rds.amazonaws.com:1521/ORACLEDB"
    val prop = new java.util.Properties()
    prop.setProperty("user", "ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    val query = "select * from asl"
    val df = spark.read.jdbc(host, query, prop)
    val df1 = spark.read.jdbc(host,"nep",prop).withColumnRenamed("name","name1")
    df.show()
    //Above code illustrates, pulling 'nep', 'asl' table data from ORACLEDB
    val createASLDDL = """create TEMPORARY VIEW asl (age int, city varchar, name varchar PRIMARY KEY);
    USING org.apache.spark.sql.cassandra
    OPTIONS (
    table "asl",
    keyspace "akshaydb",
    cluster "Test Cluster",
    pushdown "true")"""
    val createNEPDDL = """create TEMPORARY VIEW nep (ename varchar, email varchar, phone int PRIMARY KEY);
    USING org.apache.spark.sql.cassandra
    OPTIONS (
    table "nep",
    keyspace "akshaydb",
    cluster "Test Cluster",
    pushdown "true")"""
    spark.sql(createASLDDL)
    spark.sql(createNEPDDL)
    //Above code illustrated, creating a 'asl', 'nep' table in 'akshaydb' database of CASSANDRA
    df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .option("table" ,"asl").option( "keyspace" , "akshaydb").save()
    df1.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .option("table" ,"nep").option( "keyspace" , "akshaydb").save()
    //Above code illustrates, writing data to 'asl, 'nep' tables in CASSANDRA
    val join = df1.join(df,df1("name1")===df("name"),"inner").select("age","city","ename","email","phone")
    //Performing INNERJOIN. As joins are not allowed in CASSANDRA, I am integrating SPARK with CASSANDRA to perform joins.
    //Beacause cassandra need to check CONSISTENCY and so many other things that RDBMS does and so you loose PERFORMANCE, SCALABILITY that cassandra offers.
    val createDDL = """create TEMPORARY VIEW asljoinnep (age int, city varchar, ename varchar, email varchar, phone int PRIMARY KEY);
    USING org.apache.spark.sql.cassandra
    OPTIONS (
      table "asljoinnep",
    keyspace "akshaydb",
    cluster "Test Cluster",
    pushdown "true")"""
    spark.sql(createDDL)
    //Creating 'asljoinnep' table in CASSANDRA to store join results.
    join.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .option("table" ,"asljoinnep").option( "keyspace" , "akshaydb").save()
    //writing data to 'asljoinnep' table in CASSANDRA
    spark.stop()
  }
}


/*
Summary :
I have 'asl', 'nep' tables in OracleDB. I want to pull these two tables data to CASSANDRA and perform JOINS using SPARK.

Points to be Noted:
It is possible to update in Cassandra but not like SQL.
Any NoSQL database is primary key friendly. So we can update records with help of Primary Key only. 
Group By, Joins is not supported in Cassandra.
Reason :
Beacause cassandra need to check CONSISTENCY and so many other things that RDBMS does and so you loose PERFORMANCE, SCALABILITY that cassandra offers.
Solution :
Use APACHE SPARK with Cassandra.

Dependency :
1) Add below one to build.sbt
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"
2) Add below packages
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
3) Add oracle jdbc driver
4) Spark submit command
spark-submit --class com.bigdata.spark.Git.SparkINTEGRATIONCassandra --master local --deploy-mode --jars
$(echo file://home/hadoop/drivers/*.jar | tr '' ',') file://home/hadoop/sparkpoc 2.11-0.1.jar


 */