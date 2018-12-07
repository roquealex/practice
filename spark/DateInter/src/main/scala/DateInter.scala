import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
//import java.sql.Timestamp
import java.util.Date
import java.util.TimeZone

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
    .load("mini.csv")

  staticDataFrame.printSchema()
  staticDataFrame.show()

  // Extracting only the variables I'm interested
  //var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindDirectionDegrees", "WindSpeedMPH", "WindSpeedGustMPH")
  var windTimeSeriesDF = staticDataFrame.select(  "Time", "WindSpeedMPH")
  windTimeSeriesDF.show()


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

  def inter5Range(tsX:Date,tsY:Date) : Seq[Date] = {
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
    (((x+FIVE_MINUTES_IN_MILLIS-1)/FIVE_MINUTES_IN_MILLIS)*FIVE_MINUTES_IN_MILLIS to (y-1) by FIVE_MINUTES_IN_MILLIS).map(new Date(_))
    /*
     */
  }

  //val range = inter5Range(time1,time2)
  //range.foreach(println)

}
