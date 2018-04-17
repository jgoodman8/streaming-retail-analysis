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
import es.dmr.uimp.clustering.TrainInvoices
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD


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

    val sparkConf = new SparkConf().setAppName("InvoicePipeline").setMaster("local") // TODO: Remove setMaster
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(20))

    // Check-pointing
    streamingContext.checkpoint(CHECKPOINT_PATH)

    // TODO: Load model and broadcast
    val kMeansData = loadKMeansAndThreshold(sparkContext, modelFile, thresholdFile)
    val kMeansModel = streamingContext.sparkContext.broadcast(kMeansData._1)
    val kmeansThreshold = streamingContext.sparkContext.broadcast(kMeansData._2)

    val bisectionKMeansData = loadBisectingKMeansAndThreshold(sparkContext, modelFileBisect, thresholdFileBisect)
    val bisectionKMeans = streamingContext.sparkContext.broadcast(bisectionKMeansData._1)
    val bisectionThreshold = streamingContext.sparkContext.broadcast(bisectionKMeansData._2)

    // TODO: Build pipeline


    // connect to kafka
    val purchasesFeed: DStream[(String, String)] = connectToPurchases(streamingContext, zookeeperCluster, group, topics, numThreads)

    // TODO: rest of pipeline
    val purchases: DStream[Purchase] = parsePurchases(purchasesFeed)
    purchases.filter { item =>
      !item.invoiceNo.isEmpty &&
        !item.quantity.isNaN &&
        !item.invoiceDate.isEmpty &&
        !item.unitPrice.isNaN &&
        !item.customerID.isEmpty &&
        !item.country.isEmpty
    }

    //    .foreachRDD { purchase =>
    //      val parsedPurchases = purchase.
    //
    //      val wrongInvoices = purchase.filter()
    //    }

    streamingContext.start() // Start the computation
    streamingContext.awaitTermination()
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
    val threshold = rawData.map { line => line.toDouble }.first()

    threshold
  }

  def connectToPurchases(streamingContext: StreamingContext, zookeeperQuorum: String, groupId: String, topics: String,
                         numThreads: String): DStream[(String, String)] = {

    streamingContext.checkpoint(CHECKPOINT_PATH)

    // It only returns a single tuple (done due needed input format in createStream function)
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    KafkaUtils.createStream(streamingContext, zookeeperQuorum, groupId, topicMap)
  }

  def parsePurchases(feed: DStream[(String, String)]): DStream[Purchase] = {
    val csvParserSettings = new CsvParserSettings()
    csvParserSettings.detectFormatAutomatically()
    val csvParser = new CsvParser(csvParserSettings)

    val purchases: DStream[Purchase] = feed.map { item =>
      csvParser.parseRecord(item._2).asInstanceOf[Purchase]
    }

    purchases
  }
}
