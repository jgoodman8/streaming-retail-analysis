package es.dmr.uimp.realtime

import java.util.HashMap

import es.dmr.uimp.realtime.InvoicePipeline.CHECKPOINT_PATH
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaConsumer {

  def connectToKafka(streamingContext: StreamingContext, zookeeperQuorum: String, groupId: String, topics: String,
                     numThreads: String): DStream[(String, String)] = {

    streamingContext.checkpoint(CHECKPOINT_PATH)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    KafkaUtils.createStream(streamingContext, zookeeperQuorum, groupId, topicMap)
  }

  def publishToKafka(topic: String, kafkaBrokers: Broadcast[String], rdd: RDD[(String, String)]) = {
    rdd.foreachPartition(partition => {
      val kafkaConfiguration = createKafkaConfiguration(kafkaBrokers.value)
      val producer = new KafkaProducer[String, String](kafkaConfiguration)

      partition.foreach(record => {
        producer.send(new ProducerRecord[String, String](topic, record._1, record._2.toString))
      })

      producer.close()
    })
  }

  def createKafkaConfiguration(brokers: String) = {
    val properties = new HashMap[String, Object]()

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    properties
  }
}