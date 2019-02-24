package org.apache.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Runner {

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("it's need at least one parameter")
    }
    val filedirectory = args(0)


    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    println("Read JSON Content: ")
    val df = spark.read.json(filedirectory + "/input/sample-1-line.json")
    df.show(200, false)

    //df.printSchema()

    println("dfflattened - Flatten Data")
    val dfflattened = df
      .withColumn("event_ts", explode(df("dates")))
      .withColumn("cont", explode(df("content")))
    dfflattened.show(100, false)
    dfflattened.printSchema()

    println("dfflattened3 - Flatten Data")
    val dfflattened3 = dfflattened.selectExpr(/*"dates[0]",*/ "user_id", "cont.bar", "cont.foo", "event_ts")
    dfflattened3.show(100, false)
    dfflattened3.printSchema()

    println("dfconcat:")
    val byUser = Window.partitionBy("user_id")
    val dfconcat = dfflattened3.select(col("user_id"), col("event_ts").cast("timestamp"),
      concat(lit("{\""), col("bar"), lit("\"=\""), col("foo"), lit("\",\"event_ts\":\""), col("event_ts").cast("timestamp"), lit("\"}")).alias("concat_feature_value"))
      .withColumn("max_event_ts", max(col("event_ts").cast("timestamp")) over byUser)
    dfconcat.show(100, false)

    println("dfFilterOutOldEvent - Filter out old Events (event_ts < max_event_ts - 2 days)")
    val dfFilterOldEvent = dfconcat.filter(col("event_ts") < date_sub(col("max_event_ts"), 2))
    dfFilterOldEvent.show(100, false)

    println("dfFilter - Filter Result:")
    val dfFilter = dfconcat.join(dfFilterOldEvent, (dfconcat("user_id") === dfFilterOldEvent("user_id")) and (dfconcat("event_ts") === dfFilterOldEvent("event_ts")), "leftanti")
    dfFilter.show(100, false)

    println("dfgroupbyvaues - group by data based on user_id")
    dfFilter.createOrReplaceTempView("grp_by")
    val dfgroupbyvaues = spark.sql("select user_id, collect_list(concat_feature_value) as hbase_result from grp_by group by user_id")
    dfgroupbyvaues.show(100, false)

    dfflattened3.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ";")
      .option("charset", "UTF-8")
      .mode(saveMode = "overwrite")
      .save(filedirectory + "/output/E_S_result.csv")

    spark.stop
    spark.close()
  }

}
