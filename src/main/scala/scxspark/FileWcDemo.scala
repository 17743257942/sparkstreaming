package scxspark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWcDemo {
  def main(args: Array[String]): Unit = {

    val filePath = "file:///E:\\Program Files\\vm_linux\\share\\spark_jar\\monitored\\"
    val sparkConf = new SparkConf()
      .setAppName("NetworkWordCount")
      .setMaster("local[2]").set("spark.testing.memory", "5000000000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream(filePath)
//    val lines = ssc.textFileStream(args(0))
    val results = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    results.print()
    ssc.start()
    ssc.awaitTermination()


  }
}
