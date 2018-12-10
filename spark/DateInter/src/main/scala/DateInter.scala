import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.sql.Timestamp
//import java.util.Date
import java.util.TimeZone
import org.apache.spark.sql.functions.{from_utc_timestamp,col,udf,lead,datediff}
import org.apache.spark.sql.expressions.Window

object DateInter extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Date interpolator")
    .getOrCreate()

  import spark.implicits._

  val staticDataFrame = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    //.load("IYUCATNT2-2016-12-31.csv")
    //.load("mini.csv")
    .load("mini3.csv")

  staticDataFrame.printSchema()
  staticDataFrame.show()

  // Extracting only the variables I'm interested
  //var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindDirectionDegrees", "WindSpeedMPH", "WindSpeedGustMPH")
  var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindSpeedMPH", "DateUTC")
  windTimeSeriesDF.show()

  var windLocalTimeDF = windTimeSeriesDF
    .withColumn("LocalTime",from_utc_timestamp(col("DateUTC"),"America/Mexico_City"))
    .withColumn("LocalDate",col("LocalTime").cast("date"))
    .withColumn("LocalTimeSec",col("LocalTime").cast("long"))
    //.withColumn("LocalTimeTS",col("LocalTime").cast("timestamp"))
    .withColumn("TimeSec",col("Time").cast("long"))
    .withColumn("UTCSec",col("DateUTC").cast("long"))
    //.withColumn("LocalTimeTS",from_utc_timestamp(col("LocalTimeSec"), "America/Merida"))



  // When I do cast from date to long spark assumes the date is in the local timezone

  // Good time zone change 2016-10-30, this shows a bit the issue: Two UTC times map to the same local and long value same
  //+-------------------+------------+-------------------+-------------------+----------+------------+----------+----------+
  //|               Time|WindSpeedMPH|            DateUTC|          LocalTime| LocalDate|LocalTimeSec|   TimeSec|    UTCSec|
  //+-------------------+------------+-------------------+-------------------+----------+------------+----------+----------+
  //|2016-10-30 01:32:00|           7|2016-10-30 06:32:00|2016-10-30 01:32:00|2016-10-30|  1477816320|1477816320|1477834320|
  //|2016-10-30 01:32:00|           4|2016-10-30 07:32:00|2016-10-30 01:32:00|2016-10-30|  1477816320|1477816320|1477837920|

  // This is one SF station:
  // KCASANFR169
  // I'm interested in date Sunday, March 13 when we lost 1 hour in pacific but should exist in merida
  // No 2:00 am
  // Even weather underground seems to have a bug with this date in march but not in the csv.
  // My CSV is confused in the time but not in time.1, this is probably an issue with the R reader which may not take
  // into account the time zone. Anyway opening with spark and see what we get

  // The UTC conversion doesn't work well with mex city and merida:
  //|               Time|WindSpeedMPH|            DateUTC|          LocalTime| LocalDate|LocalTimeSec|   TimeSec|    UTCSec|
  //+-------------------+------------+-------------------+-------------------+----------+------------+----------+----------+
  //|2016-03-13 01:00:00|           7|2016-03-13 07:00:00|2016-03-13 01:00:00|2016-03-13|  1457859600|1457859600|1457877600|
  //|2016-03-13 01:00:00|           5|2016-03-13 08:00:00|2016-03-13 03:00:00|2016-03-13|  1457863200|1457859600|1457881200|

  windLocalTimeDF.printSchema()
  windLocalTimeDF.show()

  /*
  val partitionWindow = Window.partitionBy("LocalDate").orderBy("LocalTime")
  val windLocalTimeWindowDF = windLocalTimeDF
    .withColumn("nextLocalTime",lead("LocalTime",1).over(partitionWindow))
    .withColumn("nextWindSpeedMPH",lead("WindSpeedMPH",1).over(partitionWindow))
    .na.drop()
    //.withColumn("diffTS",datediff(col("nextLocalTime"),col("LocalTime")))
    .withColumn("diffTS",col("nextLocalTime").cast("long")-col("LocalTime").cast("long"))
  windLocalTimeWindowDF.show(100)
  */


  /*
  val tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  //tsFormat.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"))
  tsFormat.setTimeZone(TimeZone.getTimeZone("America/Mexico_city"))

  var time1 = tsFormat.parse("2016-12-31 00:02:00|")
  var time2 = tsFormat.parse("2016-12-31 00:23:00|")

  println(time1)
  println(time2)
  println(time1.getTime())
  println(time2.getTime())

*/

  def inter5Range(tsX:Timestamp,tsY:Timestamp) : Seq[Timestamp] = {
  //def inter5Range(tsX:Date,tsY:Date) : Unit = {
    //val diff = y - x
    //val by5 = diff/5
    val FIVE_MINUTES_IN_MILLIS : Long = 5*60*1000;//millisecs
    val x = tsX.getTime()
    val y = tsY.getTime()
    //for (ts <- ((x+FIVE_MINUTES_IN_MILLIS-1)/FIVE_MINUTES_IN_MILLIS)*FIVE_MINUTES_IN_MILLIS to (y-1) by FIVE_MINUTES_IN_MILLIS) {
    //  println(ts)
    //}
    //.foreach(new Timestamp(_))
    (((x+FIVE_MINUTES_IN_MILLIS-1)/FIVE_MINUTES_IN_MILLIS)*FIVE_MINUTES_IN_MILLIS to (y-1) by FIVE_MINUTES_IN_MILLIS).map(new Timestamp(_))
    /*
     */
  }

  val range5UDF = udf((x:Timestamp,y:Timestamp)=>inter5Range(x,y))

  //windTZTimeSeriesDF.show()

  //val range = inter5Range(time1,time2)
  //range.foreach(println)

}
