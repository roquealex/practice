import org.apache.spark.sql.SparkSession

object XmlTest extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  val lines = spark.sparkContext.textFile("nofile")

  val counts = lines
    .flatMap(line => line.split("\\s++"))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)

}
