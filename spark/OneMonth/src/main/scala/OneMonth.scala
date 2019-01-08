import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_utc_timestamp,col,expr,udf,lead,explode,row_number,sum,greatest,hour,lit}
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp


// One day flow based on april 18 2016
// December 2012 is good for a month flow

object OneMonth extends App {


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    //.config("spark.sql.session.timeZone", "UTC")
    //.config("user.timezone", "UTC")
    .getOrCreate()

  val staticDataFrame = spark.read
    .format("csv")
    .option("header","true")
    //.option("inferSchema","true") // InferSchema becomes unreliable when having empty csvs
    //.load("month/IYUCATNT2-2012-12-01.csv")
    .load("month/")
    //.load("mini.csv")
    //.load("mini3.csv")

  //spark.conf.set("spark.sql.session.timeZone", "UTC")

  staticDataFrame.printSchema()
  staticDataFrame.show()

  val typeDataFrame = staticDataFrame.selectExpr(
    "cast(Time as timestamp)", "cast(WindSpeedMPH as double)"
  )
  //var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindDirectionDegrees", "WindSpeedMPH", "WindSpeedGustMPH")


  // The timezone change actually has some effect since the dates get converted to UTC when read?
  val daysDF = typeDataFrame.selectExpr("cast(Time as date) AS LocalDate").groupBy("LocalDate").count()
  daysDF.show()
  daysDF.printSchema()

  /*
  val badReadings = staticDataFrame.withColumn("Dates", col("Time").cast("date")).filter(col("Dates")==="2012-12-04")
  badReadings.show()
  */

  // Extracting only the variables I'm interested
  //var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindDirectionDegrees", "WindSpeedMPH", "WindSpeedGustMPH")
  var windTimeSeriesDF = typeDataFrame.select(  "Time", "WindSpeedMPH")
  //  .withColumn("epoch",col("Time").cast("long"))
  windTimeSeriesDF.show()
  windTimeSeriesDF.printSchema()

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

  // Add the day:
  var windTimeSeriesDateDF = windTimeSeriesDF.withColumn("LocalDate",col("Time").cast("date"))
  windTimeSeriesDateDF.show()
  windTimeSeriesDateDF.printSchema()
//      .withColumn("LocalDate",col("LocalTime").cast("date"))

  val partitionWindow = Window.partitionBy("LocalDate").orderBy("Time")

  val windTimeWindowDF = windTimeSeriesDateDF
    .withColumn("nextTime",lead("Time",1).over(partitionWindow))
    .withColumn("nextWindSpeedMPH",lead("WindSpeedMPH",1).over(partitionWindow))
    .na.drop()

  windTimeWindowDF.show()
  windTimeWindowDF.printSchema()

  val windTimeWindowLineDF = windTimeWindowDF
    .withColumn("m",
      expr("nextWindSpeedMPH-WindSpeedMPH")
        /(col("nextTime").cast("long")-col("Time").cast("long"))
    )
    .withColumn("b",
      (col("Time").cast("long")*col("nextWindSpeedMPH")-col("nextTime").cast("long")*col("WindSpeedMPH"))
        /(col("Time").cast("long")-col("nextTime").cast("long"))
    )
    //.withColumn("m",expr("(next_reading-reading)/(next_ts-timestamp)"))
    //.withColumn("b",expr("(timestamp*next_reading-next_ts*reading)/(timestamp-next_ts)"))
  windTimeWindowLineDF.show()
  windTimeWindowLineDF.printSchema()


  def inter5Range(tsX:Timestamp,tsY:Timestamp) : Seq[Timestamp] = {
    val FIVE_MINUTES_IN_MILLIS : Long = 5*60*1000;//millisecs
    val x = tsX.getTime()
    val y = tsY.getTime()
    (((x+FIVE_MINUTES_IN_MILLIS-1)/FIVE_MINUTES_IN_MILLIS)*FIVE_MINUTES_IN_MILLIS to (y-1) by FIVE_MINUTES_IN_MILLIS).map(new Timestamp(_))
  }

  val range5UDF = udf((x:Timestamp,y:Timestamp)=>inter5Range(x,y))

  val windTimeWindowRangeDF = windTimeWindowLineDF.withColumn("Range",range5UDF(col("Time"),col("nextTime")))
  windTimeWindowRangeDF.show(false)

  val windTimeWindowExplodeDF = windTimeWindowRangeDF.withColumn("interTime",explode(col("Range")))
  windTimeWindowExplodeDF.show()

  val windTimeWindowInterDF = windTimeWindowExplodeDF
    .withColumn("interWindSpeedMPH",expr("(cast(interTime AS long) * m) + b"))
  windTimeWindowInterDF.show()
  windTimeWindowInterDF.printSchema()

  // remanig interTime to Time to be able to use the same partition specification
  val windExtractDF = windTimeWindowInterDF.selectExpr("LocalDate","interTime AS Time","interWindSpeedMPH")
  //  .where(hour(col("Time"))>=lit(8) &&  hour(col("Time"))<=lit(18) )
  //    .withColumn("hour",hour(col("Time")))
  windExtractDF.show(300,false)
  windExtractDF.printSchema()

  // 300 min equals 5 min
  val wind15MphDF = windExtractDF.where(col("interWindSpeedMPH")>15.0)
    .withColumn("DayRow",row_number().over(partitionWindow))
    .withColumn("TimeLong",col("Time").cast("Long"))
    .withColumn("fiveMinMult",(col("TimeLong")/300).cast("long"))
    .withColumn("dayGroup",col("fiveMinMult") - col("DayRow"))
  wind15MphDF.show(200,false)

  val wind20MphDF = windExtractDF.where(col("interWindSpeedMPH")>20.0)
    .withColumn("DayRow",row_number().over(partitionWindow))
    .withColumn("TimeLong",col("Time").cast("Long"))
    .withColumn("fiveMinMult",(col("TimeLong")/300).cast("long"))
    .withColumn("dayGroup",col("fiveMinMult") - col("DayRow"))
  wind20MphDF.show(200,false)

  val wind25MphDF = windExtractDF.where(col("interWindSpeedMPH")>25.0)
    .withColumn("DayRow",row_number().over(partitionWindow))
    .withColumn("TimeLong",col("Time").cast("Long"))
    .withColumn("fiveMinMult",(col("TimeLong")/300).cast("long"))
    .withColumn("dayGroup",col("fiveMinMult") - col("DayRow"))
  wind25MphDF.show(200,false)


  // 24 periods equals 2 hours
  val wind15MphGroupsDF = wind15MphDF.groupBy("LocalDate","dayGroup").count().filter("count>24")
  //  .sort("LocalDate")
  wind15MphGroupsDF.show()

  val wind15MphTotalDF = wind15MphGroupsDF.groupBy("LocalDate").agg(sum("count").as("Total15"))
  //  .sort("LocalDate")
  wind15MphTotalDF.show()


  //|2012-12-27| 4521960|   28|
  //  |2012-12-27| 4522115|   39|

  val wind20MphTotalDF = wind20MphDF
    .groupBy("LocalDate","dayGroup").count().filter("count>24")
    .groupBy("LocalDate").agg(sum("count").as("Total20"))
  wind20MphTotalDF.show()

  val wind25MphTotalDF = wind25MphDF
    .groupBy("LocalDate","dayGroup").count().filter("count>24")
    .groupBy("LocalDate").agg(sum("count").as("Total25"))
  wind25MphTotalDF.show()

  //var summaryDF = daysDF
  var summaryDF = windExtractDF.select("LocalDate").distinct()
    .join(wind15MphTotalDF, Seq("LocalDate"),"left_outer")
    .join(wind20MphTotalDF, Seq("LocalDate"),"left_outer")
    .join(wind25MphTotalDF, Seq("LocalDate"),"left_outer")
    .na.fill(0)
    .withColumn("pseudoWindSpeedMPH",
      (col("Total25")=!=0).cast("integer")*5 +
      (col("Total20")=!=0).cast("integer")*5 +
      (col("Total15")=!=0).cast("integer")*15
    )
    .sort("LocalDate")
    //.withColumnRenamed("LocalDate","Date")

  summaryDF.show(31)
  summaryDF.printSchema()

  summaryDF.coalesce(1).write
    .option("header", "true")
    .csv("output")
  /*
    */

}
