package NY_yellow


import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataFrameFromCSVFile {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")





    //read csv with options
    val df = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv("C:/Users/eostanin/NY taxi/yellow_tripdata_2020-01.csv")



    //таблица для количества поездок за 1 день
    val count_by_day = df
      .withColumn("tpep_pickup_datetime", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("tpep_dropoff_datetime", to_date(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
      .select(col("tpep_pickup_datetime"), col("tpep_dropoff_datetime"))
      .groupBy("tpep_pickup_datetime", "tpep_dropoff_datetime").count().as("trip_count_by_day")


    //string->date , кол-во поездок по количеству пассажиров
    val group_trip = df
      .withColumn("tpep_pickup_datetime", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("tpep_dropoff_datetime", to_date(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
      .select(col("tpep_pickup_datetime").as("pick_up"), col("tpep_dropoff_datetime").as("drop_off"), col("passenger_count").as("passenger"))
      .groupBy("pick_up", "drop_off", "passenger").count().as("trip")
      .withColumn("p0", expr("if(passenger=0,trip.count,0)"))
      .withColumn("p1", expr("if(passenger=1,trip.count,0)"))
      .withColumn("p2", expr("if(passenger=2,trip.count,0)"))
      .withColumn("p3", expr("if(passenger=3,trip.count,0)"))
      .withColumn("p4", expr("if(passenger>=4,trip.count,0)"))

    //процент поездок по количеству пасс за день
    val percent_trip = group_trip.join(count_by_day, group_trip("pick_up") === count_by_day("tpep_pickup_datetime"), "inner")
      .select(col("pick_up"), col("drop_off"),
        expr("p0/trip_count_by_day.count").as("%0"),
        expr("p1/trip_count_by_day.count").as("%1p"),
        expr("p2/trip_count_by_day.count").as("%2p"),
        expr("p3/trip_count_by_day.count").as("%3p"),
        expr("p4/trip_count_by_day.count").as("%4+"))
      .where("pick_up==tpep_pickup_datetime and drop_off ==tpep_dropoff_datetime")


    percent_trip
      .write.mode(SaveMode.Overwrite).parquet("C:/Users/eostanin/NY taxi/yellow_tripdata_2020-01.parquet")

    percent_trip.show(100)
    percent_trip.printSchema()

  }


  }
