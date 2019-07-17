package scxspark

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKey {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("NetworkWordCount")
      .setMaster("local[4]").set("spark.testing.memory", "5000000000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 设置检查点目录
    ssc.checkpoint("file:///E:\\Program Files\\vm_linux\\share\\spark_jar\\checkpoint\\")
    // 监听的是s2的9999端口
    val lines = ssc.socketTextStream("192.168.56.144", 9999
      , StorageLevel.MEMORY_AND_DISK)

    val result = lines.flatMap(_.split(" ")).map(x => (x, 1))
    val state = result.updateStateByKey[Int](updateFunction _) //方法转成函数
    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction2(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    // add the new values with the previous running count to get the new count
    var newCount = 1
    if (runningCount.isEmpty) {
      newCount = 0
    } else {
      newCount = runningCount.get
    }
    println("old value: " + newCount)
    for (i <- newValues) {
      println("new value: " + i)
      newCount = newCount + i
    }
    Option(newCount)
  }


  // Seq[V], Option[S] 前者是每个key新增的值的集合，后者是当前保存的状态
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    //通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    val currentCount = newValues.sum
    // 已累加的值
    val previousCount = runningCount.getOrElse(0)
    // 返回累加后的结果。是一个Option[Int]类型
    Some(currentCount + previousCount)
  }






}
