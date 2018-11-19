import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, udf, floor, lag, lead, expr, explode, monotonically_increasing_id}

import org.apache.spark.sql.expressions.Window


object InterLag extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SQL test")
    .getOrCreate()

  import spark.implicits._

  val staticDataFrame = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("Data2.csv")

  staticDataFrame.printSchema()
  staticDataFrame.show()

  val partDF = staticDataFrame.withColumn("hundred", floor(col("timestamp")/100))
  partDF.show()

  val partitionWindow = Window.partitionBy("hundred").orderBy("timestamp")
  val lagDF = partDF
    .withColumn("next_ts",lead("timestamp",1,-1).over(partitionWindow))
    .withColumn("next_reading",lead("reading",1,-1).over(partitionWindow))
  lagDF.show()

  /*
  val idDataFrame = staticDataFrame.withColumn("reading_num", monotonically_increasing_id)
  idDataFrame.printSchema()
  idDataFrame.show()

  val idP1DataFrame = idDataFrame.withColumn("reading_num", col("reading_num")+1);
  idP1DataFrame.printSchema()

  idDataFrame.show()
  idP1DataFrame.show()

  val rangeDF = idP1DataFrame
    .join(
      idDataFrame
        .withColumnRenamed("reading","end_reading").
        withColumnRenamed("timestamp","end_timestamp"),
      idP1DataFrame.col("reading_num")===idDataFrame.col("reading_num"),
      //"left_outer"
      "inner"
    )

  // see lag function


  //rangeDF.select("reading").show()
  rangeDF.show()
  rangeDF.printSchema()
    */


  def inter5Range(x:Int,y:Int) : Seq[Int] = {
    //val diff = y - x
    //val by5 = diff/5
    ((x+4)/5)*5 to (y-1) by 5
    /*
     */
  }

  //val range = inter5Range(3,6);
  val range = inter5Range(3,4);
  range.foreach(println)

  val range5UDF = udf((x:Int,y:Int)=>inter5Range(x,y))


  val lineDF = lagDF
    .withColumn("m",expr("(next_reading-reading)/(next_ts-timestamp)"))
    .withColumn("b",expr("(timestamp*next_reading-next_ts*reading)/(timestamp-next_ts)"))
  lineDF.show()

  val interDF = lineDF.withColumn("inter_timestamp",range5UDF(col("timestamp"),col("next_ts")))
  interDF.show()

  val interExpDF = interDF.withColumn("inter_timestamp", explode(col("inter_timestamp")))
  interExpDF.show()

  val finalDF = interExpDF.select(col("inter_timestamp"), (col("inter_timestamp")*col("m")+col("b")).as("inter_reading") )
  finalDF.show()


  //val lagDF = staticDataFrame.withColumn("test",lag("timestamp",1).over(partitionWindow))
  //lagDF.show()

  //rangeDF.select("reading_num").show() // ambiguous

  //val newDf = idDataFrame.select("*").filter(col("reading") > 3)
  //val newDf = idDataFrame.select("*").filter('reading > 3) // Requires implicits
  //val newDf = idDataFrame.select("*").filter($"reading" > 3)
  //val newDf = idDataFrame.selectExpr("*","reading>3")
  //val newDf = idDataFrame.selectExpr("reading>3")
  //val newDf = idDataFrame.selectExpr("count(distinct(reading)) as diff_readings")
  //val newDf = idDataFrame.where('reading =!= 3)

  //val newDf = idDataFrame.select("reading").distinct
  //val newDf = idDataFrame.dropDuplicates("reading","timestamp")
  //newDf.show()

  //newDf.sort("reading","timestamp").show();
  //newDf.sort('reading.desc,'timestamp).limit(5).show();

  //idDataFrame.sample(false,0.8,1).show()
  //idDataFrame.describe("reading","timestamp").show()

  /*
  import org.apache.spark.sql.Row
  case class Movie(actor_name:String, movie_title:String, produced_year:String)

  val badMoviesDF = Seq( Movie(null,null,"2018"),
    Movie(null,null,null),
    Movie("John Doe","Awesome Movie","2018"),
    Movie(null,"Awesome Movie","2018"),
    Movie("Mary Jane","Awesome Movie","2018")
  ).toDF();
  badMoviesDF.printSchema()
  badMoviesDF.show()

  badMoviesDF.select(count("actor_name"), count("*")).show()

  import org.apache.spark.sql.Row

  //case class Movie(actor_name:String, movie_title:String, produced_year:Long)
  class Movie(@BeanProperty var actor_name:String, @BeanProperty var movie_title:String, @BeanProperty var produced_year:Long)

  val badMovies = Seq( Row(null,null,2018L),
    Row("John Doe","Awesome Row",2018L),
    Row(null,"Awesome Row",2018L),
    Row("Mary Jane","Awesome Row",2018L)
  )
  val badMoviesRdd = spark.sparkContext.parallelize(badMovies)
  val badMoviesDF = spark.createDataFrame(badMoviesRdd, Movie)

    //.toDF();
  badMoviesDF.printSchema()
  badMoviesDF.show()
  */

  /*
  val lines = spark.sparkContext.textFile("nofile")

  val counts = lines
    .flatMap(line => line.split("\\s++"))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)
  */

}
