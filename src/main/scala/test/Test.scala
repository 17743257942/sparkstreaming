package test

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("test0")
    conf.set("spark.testing.memory", "5000000000")
    val sc = new SparkContext(conf)
    val blackList = List("zs", "ls")
    val blackRDD = sc.parallelize(blackList).map(x => (x, true))

    println(blackRDD.collect().mkString(","))
    // (zs,true),(ls,true)
    val lines = sc.parallelize(List("zs", "ls", "ww"))
      .map(x => (x, "20180808"))
      .map(x => (x._2, x._1))
      .map(x => (x._2, x))
    // (zs,(20180808,zs)),(ls,(20180808,ls)),(ww,(20180808,ww))
    println(lines.collect().mkString(","))


    val result = lines.leftOuterJoin(blackRDD)
    // (ww,((20180808,ww),None)),(zs,((20180808,zs),Some(true))),(ls,((20180808,ls),Some(true)))
    println(result.collect().mkString(","))
    val result1 = result.filter(x => x._2._2.getOrElse() == ())  // getOrElse(false) != true
    // (ww,((20180808,ww),None))
    println(result1.collect().mkString(","))
    val result2 = result1.map(x => x._2._1)
    // (20180808,ww)
    println(result2.collect().mkString(","))


  }
}

/**
  * getOrElse()  取值，如果有的话就取到，None的话就用括号中的值代替
  */
