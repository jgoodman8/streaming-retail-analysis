package es.dmr.uimp.realtime

import com.univocity.parsers.common.record.Record
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import es.dmr.uimp.realtime.KafkaConsumer._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

object InvoicePipeline {

  val CHECKPOINT_PATH = "./checkpoint"

  case class Purchase(invoiceNo: String, quantity: Int, invoiceDate: String,
                      unitPrice: Double, customerID: String, country: String)

  case class Invoice(invoiceNo: String, avgUnitPrice: Double,
                     minUnitPrice: Double, maxUnitPrice: Double, time: Double,
                     numberItems: Double, lastUpdated: Long, lines: Int, customerId: String)


  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zookeeperCluster, group, topics, numThreads, brokers) = args

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

    val purchasesStream = kafkaFeed.transform { inputRDD =>
      inputRDD.map { input =>
        val invoiceId = input._1
        val purchaseAsString = input._2

        val purchase = parsePurchase(purchaseAsString)

        (invoiceId, purchase)
      }
    }

    //    purchasesStream
    //      .window(Seconds(40), Seconds(20))
    //      .updateStateByKey(filterPurchase)

    // TODO: rest of pipeline


    //    val res = purchasesFeed.map(item => (item._1, 1)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(4), Seconds(2), 2).map(x => (x._1, x._2.toString))
    purchasesStream
      .window(Seconds(40))
      .updateStateByKey(updateFunction)
      .map { item =>
        (item._1, item._1)
      }
      .foreachRDD { rdd =>
        publishToKafka("xd")(broadcastBrokers)(rdd)
      }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def updateFunction(purchases: Seq[Purchase], state: Option[Invoice]): Option[Invoice] = {

    //    val invoice: Invoice = purchases.to.aggregate { purchase: Purchase => {

    val previousState: Invoice = state.getOrElse(Invoice("", 0, 0, 0, 0, 0, 0, 0, ""))
    //    val currentCount = previousState.lines + state.size
    //    val average = (previousState.avgUnitPrice * previousState.lines + purchases.sum) / currentCount
    val invoiceNo = ""
    val average: Double = 0.0f
    val minimun = 0
    val maximun = 0
    val time = 0
    val numerOfItems = previousState.numberItems + 0 // purchases.aggregate(_ => _.)
    val lines = previousState.lines + state.size
    val lastUpdated = 0
    val country = ""

    Some(new Invoice(invoiceNo, average, minimun, maximun, time, numerOfItems, lines, lastUpdated, country))

    //      purchase.invoiceNo
    //      ,
    //      mean(purchase.unitPrice)
    //      ,
    //      min(purchase.unitPrice)
    //      ,
    //      max(purchase.unitPrice)
    //      ,
    //      min("UnitPrice").alias("MinUnitPrice")
    //      ,
    //      max("UnitPrice").alias("MaxUnitPrice")
    //      ,
    //      first("Hour").alias("Time")
    //      ,
    //      purchase.quantity. sum().alias("NumberItems")
    //      )
    //  }
    //}


    //Option (invoice)
    //    Option(Invoice("IDX", 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1, "CUSTOMER"))
  }

  /**
    * Given a purchases feed formatted as csv-like strings, returns those purchases parsed as objects by using a
    * csv parser
    *
    * @param purchase Feed from Kafka with purchases as csv-like strings
    * @return
    */
  def parsePurchase(purchase: String) = {
    val csvParserSettings = new CsvParserSettings()
    csvParserSettings.detectFormatAutomatically()
    val csvParser = new CsvParser(csvParserSettings)

    val parsedPurchase = csvParser.parseRecord(purchase)

    recordToPurchase(parsedPurchase)
  }

  def recordToPurchase(record: Record) = Purchase(
    record.getString(0),
    record.getInt(3),
    record.getString(4),
    record.getDouble(5),
    record.getString(6),
    record.getString(7)
  )

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

  //    def filterPurchase(newValues: Seq[Purchase], runningCount: Option[Purchase]): Option[T] = {
  //
  //    }

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
