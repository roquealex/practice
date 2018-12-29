import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_utc_timestamp,col,expr,udf,lead,explode,row_number}
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp


// One day flow based on april 18 2016
// December 2012 is good for a month flow

object OneDay extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .config("spark.sql.session.timeZone", "UTC")
    //.config("user.timezone", "UTC")
    .getOrCreate()

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  val staticDataFrame = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("IYUCATNT2-2016-04-18.csv")
    //.load("mini.csv")
    //.load("mini3.csv")

  //spark.conf.set("spark.sql.session.timeZone", "UTC")

  staticDataFrame.printSchema()
  staticDataFrame.show()

  // Extracting only the variables I'm interested
  //var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindDirectionDegrees", "WindSpeedMPH", "WindSpeedGustMPH")
  var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindSpeedMPH")
    .withColumn("epoch",col("Time").cast("long"))
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

  /*
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
  windExtractDF.show(300,false)
  windExtractDF.printSchema()

  // 300 min equals 5 min
  val wind25MphDF = windExtractDF.where(col("interWindSpeedMPH")>25.0)
    .withColumn("DayRow",row_number().over(partitionWindow))
    .withColumn("TimeLong",col("Time").cast("Long"))
    .withColumn("fiveMinMult",(col("TimeLong")/300).cast("long"))
    .withColumn("dayGroup",col("fiveMinMult") - col("DayRow"))

  wind25MphDF.show(200,false)

  val wind25MphGroupsDF = wind25MphDF.groupBy("LocalDate","dayGroup").count()
  wind25MphGroupsDF.show()


  */
}
