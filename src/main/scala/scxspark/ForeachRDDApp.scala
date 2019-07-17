package scxspark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 词频统计，并写入mysql
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("NetworkWordCount2")
      .setMaster("local[4]").set("spark.testing.memory", "5000000000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 监听的是s2的9998端口
    val lines = ssc.socketTextStream("192.168.56.144", 9998)

        val result = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    // window 算子的使用
//    val result = lines.flatMap(_.split(" ")).map(x => (x, 1))
//      .reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(30), Seconds(10))

    //写入mysql
    result.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          println(record._1)
          println(record._2)
          val sql = "insert into  wordcount (word,wordcount) values ('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
          println(sql)
        })
        connection.close()
      }
    }


    result.print()
    ssc.start()
    ssc.awaitTermination()

  }

  def createConnection() = {

    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_test?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC",
      "root", "root")

  }


}
