package scxspark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 访问日志：==> DStream
  * 20180808,zs
  * 20180808,ls
  * 20180808,ww
  * 先转化：(zs:20180808,zs)(ls:20180808,ls)(ww:20180808,ww)
  *
  *
  * 黑名单列表： ==> RDD
  * zs
  * ls
  * 先转化：(zs:true)(ls:true)
  *
  * leftjoin
  * (zs:[<20180808,zs>,<true>])
  * (ls:[<20180808,ls>,<true>])
  * (ww:[<20180808,ww>,<false>])  ==> tuple 1
  *
  *
  * 结果：
  * 20180808,ww
  *
  */
object BlackList {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("NetworkWordCount")
      .setMaster("local[4]")
      .set("spark.testing.memory", "5000000000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /**
      * 构建黑名单
      */
    val blackList = List("zs", "ls")
    val blackRDD = ssc.sparkContext.parallelize(blackList).map(x => (x, true))

    val lines = ssc.socketTextStream("192.168.56.144", 9998)
    val result = lines.map(x => (x.split(",")(1), x)).transform(
      rdd => {
        rdd.leftOuterJoin(blackRDD).filter(x => x._2._2.getOrElse(false) != true)
          .map(x => x._2._1)
      })
    result.print()

    ssc.start()
    ssc.awaitTermination()


  }
}
