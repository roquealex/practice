import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_utc_timestamp,col,lit,expr,udf,lead,explode,row_number,to_timestamp}
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp

object TzTest extends App {


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    //.config("spark.sql.session.timeZone", "UTC")
    //.config("spark.sql.session.timeZone", "America/Merida")
    //.config("user.timezone", "UTC")
    .getOrCreate()

  // For databricks:
  spark.conf.set("spark.sql.session.timeZone", "America/Merida")

  //TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  val staticDataFrame = spark.read
    .format("csv")
    .option("header","true")
    //.option("inferSchema","true")
    //.load("IYUCATNT2-2016-04-18.csv")
    //.load("oneday.csv")
    .load("selected/*.csv")
  //.load("mini.csv")
  //.load("mini3.csv")

  //spark.conf.set("spark.sql.session.timeZone", "UTC")

  staticDataFrame.printSchema()
  staticDataFrame.show()

  val sortUtcDF = staticDataFrame.sort("DateUTC")
  //sortUtcDF.show()

  // Version 1 : select Time.1 as the timestamp and use to_timestamp
  val windTimeSeriesDF = sortUtcDF.selectExpr(
    "`Time.1`",
    "DateUTC",
    "to_timestamp(`Time.1`,'yyyy-MM-dd HH:mm:ss') as TS")
    .withColumn("epoch_min",col("TS").cast("long").divide(lit(60)).cast("long"))
  // Version 1 has issues on this range that gets repeated for different UTC:
  //|2016-10-30 01:03:00|2016-10-30 06:03:00|2016-10-30 01:03:00| 24630183|
  //|2016-10-30 01:32:00|2016-10-30 06:32:00|2016-10-30 01:32:00| 24630212|
  //|2016-10-30 01:58:00|2016-10-30 06:58:00|2016-10-30 01:58:00| 24630238|
  //|2016-10-30 01:03:00|2016-10-30 07:03:00|2016-10-30 01:03:00| 24630183|
  //|2016-10-30 01:32:00|2016-10-30 07:32:00|2016-10-30 01:32:00| 24630212|
  //|2016-10-30 01:58:00|2016-10-30 07:58:00|2016-10-30 01:58:00| 24630238|
  /*
  */


  /*
  // Date UTC
  // Version 2 : select DateUTC as the timestamp and use to_timestamp
  val windTimeSeriesDF = sortUtcDF.selectExpr(
    "`Time.1`",
    "DateUTC",
    "from_utc_timestamp(DateUTC,'America/Merida') as TS")
    .withColumn("epoch_min",col("TS").cast("long").divide(lit(60)).cast("long"))
  // version 2 has way more issues, it looks like the string date at some point gets converted to the local machine time
  // At some point this one breaks:
  //|2016-03-12 21:30:00|2016-03-13 03:30:00|2016-03-12 21:30:00| 24297330|
  //|2016-03-13 00:00:00|2016-03-13 06:00:00|2016-03-13 01:00:00| 24297540|
  */


  // Display and save:
  val size : Int = windTimeSeriesDF.count().toInt
  windTimeSeriesDF.show(size)

  // Results goes in here:
  //windTimeSeriesDF.coalesce(1).write.option("header","true").csv("output")
  // To compare against databricks:
  windTimeSeriesDF.select("`Time.1`","epoch_min").coalesce(1).write.option("header","true").csv("output")


/*
// Extracting only the variables I'm interested
//var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindDirectionDegrees", "WindSpeedMPH", "WindSpeedGustMPH")
//var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindSpeedMPH")
//var windTimeSeriesDF = staticDataFrame.selectExpr(  "cast(Time as timestamp)", "WindSpeedMPH")
//var windTimeSeriesDF = staticDataFrame.selectExpr(  "to_date(Time)", "WindSpeedMPH")
var windTimeSeriesDF = staticDataFrame.select(  to_timestamp(col("`Time.1`"),"yyyy-MM-dd HH:mm:ss").as("Time"), col("WindSpeedMPH"))
//var windTimeSeriesDF = staticDataFrame.select(from_utc_timestamp(col("DateUTC"),"America/Merida").as("Time"), col("WindSpeedMPH"))
  .withColumn("epoch",col("Time").cast("long"))
windTimeSeriesDF.show()
windTimeSeriesDF.printSchema()
*/

// Create a data set with many problematic dates and epoch time and april 18 as reference

// Daylight saving time 2016 in Mexico began at 2:00 AM on
//Sunday, April 3
//and ended at 2:00 AM on
//Sunday, October 30
//All times are in Central Time.

// Daylight saving time 2016 in California began at 2:00 AM on
//Sunday, March 13
//and ended at 2:00 AM on
//Sunday, November 6
//All times are in Pacific Time.

// Methodology.
// pick time stamps around the time change of the days that belong to merida timezone
// pick time stamps around the time change of the days that belong to pacific timezone
// pick timestamps in UTC column that map to timestamps around the time change (for instance UTC time 2 am in april 3
// pick my reference dates in april 18 and the epoch time

// This is the current golden using strings and changing the TZ config:
//+-------------------+------------+----------+
//|               Time|WindSpeedMPH|     epoch|
//+-------------------+------------+----------+
//|1970-01-01 00:00:00|           7|     21600|
//|2016-04-18 00:00:00|           7|1460955600|
//|2016-04-18 01:25:00|           6|1460960700|

// When using the epoch var in merida:
//+-------------------+------------+----------+
//|               Time|WindSpeedMPH|     epoch|
//+-------------------+------------+----------+
//|2016-04-18 00:00:00|           7|1460955600|

// When using the epoch var in databricks:
//+-------------------+------------+----------+
//|               Time|WindSpeedMPH|     epoch|
//+-------------------+------------+----------+
//|2016-04-18 00:00:00|           7|1460937600|

// Using the timezone command:
// spark.conf.set("spark.sql.session.timeZone", "UTC")
//+-------------------+------------+----------+
//|               Time|WindSpeedMPH|     epoch|
//+-------------------+------------+----------+
//|2016-04-18 05:00:00|           7|1460955600|


}
