package wordcount

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object WordCount {
 def main(args: Array[String]) = {
  val mode = args(0)
  val props = ConfigFactory.load()
  val envProps = props.getConfig(mode)

  val spark = SparkSession.
    builder.
    master(envProps.getString("execution.mode")).
    appName("Spark Scala word count program").
    getOrCreate()
  val inputdir = envProps.getString(("input.base.dir"))
  val outputdir = envProps.getString("output.base.dir")
  val sc = spark.sparkContext
  val lines = sc.textFile(inputdir)
  val words = lines.flatMap(l => l.split(" "))
  val word = words.map(w => (w, 1))
  val words_cnt = word.reduceByKey((tot, ele) => tot + ele)
  words_cnt.coalesce(1).saveAsTextFile(outputdir)
  spark.stop()
 }
}
