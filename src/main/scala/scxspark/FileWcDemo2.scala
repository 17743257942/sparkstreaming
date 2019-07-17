package scxspark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWcDemo2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val lines = ssc.textFileStream(args(0))
    val results = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    results.print()
    ssc.start()
    ssc.awaitTermination()



  }
}
