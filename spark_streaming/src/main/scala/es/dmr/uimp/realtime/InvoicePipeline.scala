package es.dmr.uimp.realtime

import com.univocity.parsers.common.record.Record
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import es.dmr.uimp.clustering.TrainInvoices._
import es.dmr.uimp.realtime.KafkaConsumer._
import es.dmr.uimp.realtime.Loaders._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object InvoicePipeline {

  val CANCELLATIONS_TOPIC = "cancelaciones"
  val WRONG_INVOICES_TOPIC = "facturas_erroneas"
  val K_MEANS_ANOMALIES_TOPIC = "anomalias_kmeans"
  val BISECTION_K_MEANS_ANOMALIES_TOPIC = "anomalias_bisect_kmeans"
  val CHECKPOINT_PATH = "./checkpoint"

  case class Purchase(invoiceNo: String, quantity: Int, invoiceDate: String,
                      unitPrice: Double, customerID: String, country: String)

  case class Invoice(invoiceNo: String, avgUnitPrice: Double,
                     minUnitPrice: Double, maxUnitPrice: Double, time: Double,
                     numberItems: Double, lastUpdated: Long, lines: Int, customerId: String)


  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zookeeperCluster, group, topics, numThreads, brokers) = args

    val sparkConf = new SparkConf().setAppName("InvoicePipeline").setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(1))
    streamingContext.checkpoint(CHECKPOINT_PATH)

    // TODO: Load model and broadcast
    val kMeansData = loadKMeansAndThreshold(sparkContext, modelFile, thresholdFile)
    val kMeansModel: Broadcast[KMeansModel] = streamingContext.sparkContext.broadcast(kMeansData._1)
    val kMeansThreshold: Broadcast[Double] = streamingContext.sparkContext.broadcast(kMeansData._2)

    val bisectionKMeansData = loadBisectingKMeansAndThreshold(sparkContext, modelFileBisect, thresholdFileBisect)
    val bisectionKMeans: Broadcast[BisectingKMeansModel] = streamingContext.sparkContext.broadcast(bisectionKMeansData._1)
    val bisectionThreshold: Broadcast[Double] = streamingContext.sparkContext.broadcast(bisectionKMeansData._2)

    val broadcastBrokers: Broadcast[String] = streamingContext.sparkContext.broadcast(brokers)

    // TODO: Build pipeline

    val kafkaFeed: DStream[(String, String)] = connectToKafka(streamingContext, zookeeperCluster, group, topics, numThreads)

    val purchasesStream = getPurchasesStream(kafkaFeed)

    // TODO: rest of pipeline
    detectWrongPurchases(purchasesStream, broadcastBrokers)
    detectCancellations(purchasesStream, broadcastBrokers)

    val invoices = purchasesStream
      .window(Seconds(40), Seconds(1))
      .updateStateByKey(calculateInvoice)

    detectAnomaly(invoices, kMeansModel, kMeansThreshold, BISECTION_K_MEANS_ANOMALIES_TOPIC, broadcastBrokers)
    detectAnomaly(invoices, bisectionKMeans, bisectionThreshold, BISECTION_K_MEANS_ANOMALIES_TOPIC, broadcastBrokers)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getPurchasesStream(kafkaFeed: DStream[(String, String)]): DStream[(String, Purchase)] = {
    val purchasesStream = kafkaFeed.transform { inputRDD =>
      inputRDD.map { input =>
        val invoiceId = input._1
        val purchaseAsString = input._2

        val purchase = parsePurchase(purchaseAsString)

        (invoiceId, purchase)
      }
    }

    purchasesStream
  }

  def detectWrongPurchases(purchasesStream: DStream[(String, Purchase)], broadcastBrokers: Broadcast[String]) = {
    purchasesStream
      .filter(tuple => isWrongPurchase(tuple._2))
      .transform { invoicesTupleRDD => // TODO: Refactor this :9
        invoicesTupleRDD.map(count => (count.toString, count.toString))
      }
      .foreachRDD { rdd =>
        publishToKafka(WRONG_INVOICES_TOPIC, broadcastBrokers, rdd)
      }
  }

  def detectCancellations(purchasesStream: DStream[(String, Purchase)], broadcastBrokers: Broadcast[String]) = {
    purchasesStream
      .filter(tuple => isCancellation(tuple._2))
      .countByWindow(Seconds(8), Seconds(1))
      .transform { invoicesTupleRDD => // TODO: Refactor this :9
        invoicesTupleRDD.map(count => (count.toString, count.toString))
      }
      .foreachRDD { rdd =>
        publishToKafka(CANCELLATIONS_TOPIC, broadcastBrokers, rdd)
      }
  }

  def calculateInvoice(purchases: Seq[Purchase], state: Option[Invoice]): Option[Invoice] = {

    val invoiceNo = purchases.head.invoiceNo
    val unitPrices = purchases.map(purchase => purchase.unitPrice)

    val average: Double = unitPrices.sum / unitPrices.length
    val minimum: Double = unitPrices.min
    val maximum: Double = unitPrices.max
    val time = getHour(purchases.head.invoiceDate)
    val numberOfItems = purchases.map(purchase => purchase.quantity).sum
    val lines = purchases.length
    val lastUpdated = 0
    val customer = purchases.head.customerID

    Some(Invoice(invoiceNo, average, minimum, maximum, time, numberOfItems, lines, lastUpdated, customer))
  }

  def getHour(dateTime: String): Double = {
    var hour: Double = 0.0f

    val parsedDateTime = dateTime.substring(10).split(":")(0)
    if (!StringUtils.isEmpty(parsedDateTime))
      hour = parsedDateTime.trim.toDouble

    hour
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

  def detectAnomaly(invoices: DStream[(String, Invoice)], model: Any, threshold: Broadcast[Double],
                    topic: String, broadcastBrokers: Broadcast[String]) = {
    invoices
      .filter { tuple =>
        isAnomaly(tuple._2, model, threshold)
      }
      .transform { invoicesTupleRDD => // TODO: Refactor this :9
        invoicesTupleRDD.map { invoiceTuple =>
          val invoiceNo = invoiceTuple._1

          (invoiceNo, invoiceNo)
        }
      }
      .foreachRDD { rdd =>
        publishToKafka(topic, broadcastBrokers, rdd)
      }
  }

  private def isAnomaly(invoice: Invoice, model: Any, threshold: Broadcast[Double]): Boolean = {
    val featuresBuffer = ArrayBuffer[Double]()

    featuresBuffer.append(invoice.avgUnitPrice)
    featuresBuffer.append(invoice.minUnitPrice)
    featuresBuffer.append(invoice.maxUnitPrice)
    featuresBuffer.append(invoice.time)
    featuresBuffer.append(invoice.numberItems)

    val features = Vectors.dense(featuresBuffer.toArray)

    val distance = model match {
      case model: KMeansModel => distToCentroidFromKMeans(features, model.asInstanceOf[KMeansModel])
      case model: BisectingKMeansModel => distToCentroidFromBisectingKMeans(features, model.asInstanceOf[BisectingKMeansModel])
    }

    distance.>(threshold.value)
  }

  private def isCancellation(purchase: Purchase): Boolean = {
    purchase.quantity.<(0)
  }

  private def isWrongPurchase(purchase: Purchase): Boolean = {
    purchase.invoiceNo == null || purchase.invoiceDate == null || purchase.customerID == null ||
      purchase.invoiceNo.isEmpty || purchase.invoiceDate.isEmpty || purchase.customerID.isEmpty ||
      purchase.unitPrice.isNaN || purchase.quantity.isNaN || purchase.country.isEmpty ||
      purchase.unitPrice.<(0)
  }
}
