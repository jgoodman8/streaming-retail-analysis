package es.dmr.uimp.realtime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.HashMap

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import es.dmr.uimp.clustering.TrainInvoices.{distToCentroidFromBisectingKMeans, distToCentroidFromKMeans}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator


object InvoicePipeline {

  val CHECKPOINT_PATH = "./checkpoint"

  case class Purchase(invoiceNo: String, quantity: Int, invoiceDate: String,
                      unitPrice: Double, customerID: String, country: String)

  case class Invoice(invoiceNo: String, avgUnitPrice: Double,
                     minUnitPrice: Double, maxUnitPrice: Double, time: Double,
                     numberItems: Double, lastUpdated: Long, lines: Int, customerId: String)


  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zookeeperCluster, group, topics,
    numThreads, brokers) = args

    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(1))
    streamingContext.checkpoint(CHECKPOINT_PATH)

    // TODO: Load model and broadcast
    //    val kMeansData = loadKMeansAndThreshold(sparkContext, modelFile, thresholdFile)
    //    val kMeansModel: Broadcast[KMeansModel] = streamingContext.sparkContext.broadcast(kMeansData._1)
    //    val kmeansThreshold: Broadcast[Double] = streamingContext.sparkContext.broadcast(kMeansData._2)

    //    val bisectionKMeansData = loadBisectingKMeansAndThreshold(sparkContext, modelFileBisect, thresholdFileBisect)
    //    val bisectionKMeans: Broadcast[BisectingKMeansModel] = streamingContext.sparkContext.broadcast(bisectionKMeansData._1)
    //    val bisectionThreshold: Broadcast[Double] = streamingContext.sparkContext.broadcast(bisectionKMeansData._2)

    val broadcastBrokers: Broadcast[String] = streamingContext.sparkContext.broadcast(brokers)

    // TODO: Build pipeline

    val kafkaFeed: DStream[(String, String)] = connectToKafka(streamingContext, zookeeperCluster, group, topics, numThreads)

    //    val res = purchasesFeed.map(item => (item._1, 1)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(4), Seconds(2), 2).map(x => (x._1, x._2.toString))
    kafkaFeed
      .window(Seconds(5))
      .updateStateByKey(updateFunction)
      .map({ item =>
        (item._1, item._1)
      })
      .foreachRDD({ rdd =>
        publishToKafka("ts")(broadcastBrokers)(rdd)
      })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def updateFunction(newValues: Seq[String], runningCount: Option[Invoice]): Option[Invoice] = {
    Option(Invoice("IDX", 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1, "CUSTOMER"))
  }

  def publishToKafka(topic: String)(kafkaBrokers: Broadcast[String])(rdd: RDD[(String, String)]) = {
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

  /**
    * Load the model information: centroid and threshold for kMeans model
    *
    * @param sparkContext  Spark context reference
    * @param modelFile     Route to the folder where the generated kMeans model is stored
    * @param thresholdFile Route to the folder where the generated kMeans threshold is stored
    * @return
    */
  def loadKMeansAndThreshold(sparkContext: SparkContext, modelFile: String, thresholdFile: String): (KMeansModel, Double) = {
    val kMeansModel = KMeansModel.load(sparkContext, modelFile)

    val threshold = loadThresholdFromFile(sparkContext, thresholdFile)

    (kMeansModel, threshold)
  }

  /**
    * Load the model information: centroid and threshold for bisecting kMeans model
    *
    * @param sparkContext  Spark context reference
    * @param modelFile     Route to the folder where the generated bisecting kMeans model is stored
    * @param thresholdFile Route to the folder where the generated bisecting kMeans threshold is stored
    * @return
    */
  def loadBisectingKMeansAndThreshold(sparkContext: SparkContext, modelFile: String,
                                      thresholdFile: String): (BisectingKMeansModel, Double) = {
    val bisectingKMeansModel = BisectingKMeansModel.load(sparkContext, modelFile)

    val bisectingThreshold = loadThresholdFromFile(sparkContext, thresholdFile)

    (bisectingKMeansModel, bisectingThreshold)
  }

  /**
    * Loads a threshold from a given file url
    *
    * @param sparkContext  Spark context reference
    * @param thresholdFile Route to the folder where the threshold is stored
    * @return
    */
  def loadThresholdFromFile(sparkContext: SparkContext, thresholdFile: String): Double = {
    val rawData = sparkContext.textFile(thresholdFile, 20)
    val threshold = rawData.map {
      line => line.toDouble
    }.first()

    threshold

  }

  def connectToKafka(streamingContext: StreamingContext, zookeeperQuorum: String, groupId: String, topics: String,
                     numThreads: String): DStream[(String, String)] = {

    streamingContext.checkpoint(CHECKPOINT_PATH)

    // It only returns a single tuple (done due needed input format in createStream function)
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    KafkaUtils.createStream(streamingContext, zookeeperQuorum, groupId, topicMap)
  }

  /**
    * Given a purchases feed formatted as csv-like strings, returns those purchases parsed as objects by using a
    * csv parser
    *
    * @param purchase Feed from Kafka with purchases as csv-like strings
    * @return
    */
  def parsePurchase(purchase: (String, String)) = {
    val csvParserSettings = new CsvParserSettings()
    csvParserSettings.detectFormatAutomatically()
    val csvParser = new CsvParser(csvParserSettings)

    val parsed = csvParser.parseRecord(purchase._2)

    parsed
  }

  def detectAnomaliesWithKMeans(invoicesFeed: Invoice, model: Broadcast[KMeansModel], threshold: Broadcast[Float]) = {
    //    val anomalies = invoicesFeed.foreachRDD { invoice =>
    //      invoice.
    //      val invoiceProperties = Vector(invoice.invoiceNo, invoice.avgUnitPrice, invoice.minUnitPrice,
    //        invoice.maxUnitPrice, invoice.time, invoice.numberItems)
    //      invoice.
    //      val distance: Double = distToCentroidFromKMeans(invoice, model.value)
    //      distance.>(threshold.value)
    //  }
  }

  //  class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  //
  //    lazy val producer = createProducer()
  //
  //    def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
  //  }
  //
  //  object KafkaSink {
  //    def apply(kafkaBrokers: Broadcast[String]): KafkaSink = {
  //      val f = () => {
  //        new KafkaProducer[String, String](createKafkaConfiguration(kafkaBrokers.value))
  //      }
  //      new KafkaSink(f)
  //    }
  //  }

  //  object AccCounter {
  //
  //    @volatile private var instance: LongAccumulator = null
  //
  //    def getInstance(sc: SparkContext): LongAccumulator = {
  //      if (instance == null) {
  //        synchronized {
  //          if (instance == null) {
  //            instance = sc.longAccumulator("AccCounter")
  //          }
  //        }
  //      }
  //      instance
  //    }
  //  }
}
