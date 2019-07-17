package scxspark.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * push方式整合flume
  */
object FlumePushWC {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("FlumePushWC")
      .setMaster("local[4]")
      .set("spark.testing.memory", "5000000000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val flumeStream = FlumeUtils.createStream(ssc, "192.168.1.5", 41414)

    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .print()


    ssc.start()
    ssc.awaitTermination()


  }
}
