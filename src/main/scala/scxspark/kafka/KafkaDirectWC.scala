package scxspark.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * sparkstreaming对接kafka  没有receiver模式，只有Direct
  */
object KafkaDirectWC {
  def main(args: Array[String]): Unit = {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkConf = new SparkConf()
      .setAppName("KafkaDirectWC")
      .setMaster("local[4]")
      .set("spark.testing.memory", "5000000000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    // 构建kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.56.145:9092",
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val topics = List("kafka_streaming_topic")

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    /**
      * ConsumerRecord(topic = kafka_streaming_topic, partition = 0, offset = 8,
      * CreateTime = 1563163878789, checksum = 2543104548, serialized key size = -1,
      * serialized value size = 1, key = null, value = aaaa)
      */
    kafkaStream.print()

    /**
      * (null,aaaa)
      * aaaa
      * 输入 a a a    输出 (a,3)
      */
    kafkaStream.map(record => (record.key, record.value)).print()
    kafkaStream.map(record => (record.value)).print()
    kafkaStream.map(record => (record.value))
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .print()




    ssc.start()
    ssc.awaitTermination()


  }
}
