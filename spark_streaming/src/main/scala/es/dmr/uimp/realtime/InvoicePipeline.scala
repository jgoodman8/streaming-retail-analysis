package es.dmr.uimp.realtime

import es.dmr.uimp.realtime.KafkaConsumer._
import es.dmr.uimp.realtime.Loaders._
import es.dmr.uimp.realtime.Model._
import es.dmr.uimp.realtime.Utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}

object InvoicePipeline {

  val CANCELLATIONS_TOPIC = "cancelaciones"
  val WRONG_INVOICES_TOPIC = "facturas_erroneas"
  val K_MEANS_ANOMALIES_TOPIC = "anomalias_kmeans"
  val BISECTION_K_MEANS_ANOMALIES_TOPIC = "anomalias_bisect_kmeans"
  val CHECKPOINT_PATH = "./checkpoint"

  def main(args: Array[String]) {

    // Initialization of properties
    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zookeeperCluster, group, topics, numThreads, brokers) = args

    val sparkConf = new SparkConf().setAppName("InvoicePipeline").setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(1))
    streamingContext.checkpoint(CHECKPOINT_PATH)

    // Models and thresholds load and broadcasting to worker nodes
    val kMeansData = loadKMeansAndThreshold(sparkContext, modelFile, thresholdFile)
    val kMeansModel: Broadcast[KMeansModel] = streamingContext.sparkContext.broadcast(kMeansData._1)
    val kMeansThreshold: Broadcast[Double] = streamingContext.sparkContext.broadcast(kMeansData._2)

    val bisectionKMeansData = loadBisectingKMeansAndThreshold(sparkContext, modelFileBisect, thresholdFileBisect)
    val bisectionKMeans: Broadcast[BisectingKMeansModel] = streamingContext.sparkContext.broadcast(bisectionKMeansData._1)
    val bisectionThreshold: Broadcast[Double] = streamingContext.sparkContext.broadcast(bisectionKMeansData._2)

    val broadcastBrokers: Broadcast[String] = streamingContext.sparkContext.broadcast(brokers)

    // Get feed from kafka and conversion to scala object from string
    val kafkaFeed: DStream[(String, String)] = connectToKafka(streamingContext, zookeeperCluster, group, topics, numThreads)

    val purchasesStream = getPurchasesStream(kafkaFeed)

    // Detection of cancellations and wrong purchases
    detectWrongPurchases(purchasesStream, broadcastBrokers)
    detectCancellations(purchasesStream, broadcastBrokers)

    // Creating an invoice feed from the invoices feed
    val invoices: DStream[(String, Invoice)] = purchasesStream
      .filter(tuple => !isWrongPurchase(tuple._2))
      .window(Seconds(40), Seconds(1))
      .updateStateByKey(calculateInvoice)

    // Detection of anomalies for both models: kMeans and Bisection kMeans
    detectAnomaly(invoices, kMeansModel.value, kMeansThreshold.value, K_MEANS_ANOMALIES_TOPIC, broadcastBrokers)
    detectAnomaly(invoices, bisectionKMeans.value, bisectionThreshold.value, BISECTION_K_MEANS_ANOMALIES_TOPIC, broadcastBrokers)

    // Start pipeline
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
    * Transforms the Kafka Stream into a new stream with a the value parsed as a Purchase class
    *
    * @param kafkaFeed Stream from Kafka (key: invoiceNo, value: purchase String)
    * @return
    */
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

  /**
    * Given a Purchase Stream, detects the wrong purchases (with any missing or wrong field) and sends the feedback to
    * the WRONG_INVOICES_TOPIC kafka topic
    *
    * @param purchasesStream  Stream with (key: invoiceNo, value: Purchase object)
    * @param broadcastBrokers Kafka brokers
    */
  def detectWrongPurchases(purchasesStream: DStream[(String, Purchase)], broadcastBrokers: Broadcast[String]): Unit = {
    purchasesStream
      .filter(tuple => isWrongPurchase(tuple._2))
      .transform { purchasesTupleRDD =>
        purchasesTupleRDD.map(purchase => (purchase._1.toString, purchase._2.toString))
      }
      .foreachRDD { rdd =>
        publishToKafka(WRONG_INVOICES_TOPIC, broadcastBrokers, rdd)
      }
  }

  /**
    * Given a Purchase Stream, detects the cancellations (the quantity property is negative) and calculates the number
    * of cancellations for the last 8 minutes. It gives this feedback to the CANCELLATIONS_TOPIC Kafka topic every second
    *
    * @param purchasesStream  Stream with (key: invoiceNo, value: Purchase object)
    * @param broadcastBrokers Kafka brokers
    */
  def detectCancellations(purchasesStream: DStream[(String, Purchase)], broadcastBrokers: Broadcast[String]): Unit = {
    purchasesStream
      .filter(tuple => isCancellation(tuple._2))
      .countByWindow(Minutes(8), Seconds(1))
      .transform { invoicesTupleRDD =>
        invoicesTupleRDD.map(count => (count.toString, count.toString))
      }
      .foreachRDD { rdd =>
        publishToKafka(CANCELLATIONS_TOPIC, broadcastBrokers, rdd)
      }
  }

  /**
    * For a given stream of invoices, model and threshold, detects if any invoice is an anomaly and if so, it sends
    * the feedback to the given kafka topic.
    *
    * @param invoices         Stream of invoice tuples (key: invoiceNo, value: Invoice object)
    * @param model            Model (KMeansModel | BisectionKMeanModel) we want to use to detect anomalies
    * @param threshold        The threshold related to the model
    * @param topic            Target kafka topic
    * @param broadcastBrokers Kafka brokers
    */
  def detectAnomaly(invoices: DStream[(String, Invoice)], model: Saveable, threshold: Double, topic: String,
                    broadcastBrokers: Broadcast[String]): Unit = {
    invoices
      .filter { tuple =>
        val invoice: Invoice = tuple._2
        isAnomaly(invoice, model, threshold)
      }
      .transform { invoicesTupleRDD =>
        invoicesTupleRDD.map(invoiceTuple => (invoiceTuple._1, invoiceTuple._2.toString))
      }
      .foreachRDD { rdd =>
        publishToKafka(topic, broadcastBrokers, rdd)
      }
  }
}
