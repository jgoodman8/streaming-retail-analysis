package es.dmr.uimp.realtime

import java.util.Date

import com.univocity.parsers.common.record.Record
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import es.dmr.uimp.clustering.TrainInvoices.{distToCentroidFromBisectingKMeans, distToCentroidFromKMeans}
import es.dmr.uimp.realtime.Model._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.mllib.clustering.{BisectingKMeansModel, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.Saveable

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Utils {

  /**
    * For a given sequence of purchases (owning to the same invoice), it calculates the invoice properties and returns a
    * a proper formatted object for each sequence
    *
    * @param newPurchases
    * @param previousInvoice
    * @return
    */
  def calculateInvoice(newPurchases: Seq[Purchase], previousInvoice: Invoice): Invoice = {

    val now = (new Date).getTime / 1000

    val invoiceNo = newPurchases.head.invoiceNo
    val customer = newPurchases.head.customerID
    val newUnitPrices = newPurchases.map(purchase => purchase.unitPrice)

    val maximum = if (previousInvoice.maxUnitPrice > newUnitPrices.max) previousInvoice.maxUnitPrice else newUnitPrices.max
    val minimum = if (previousInvoice.maxUnitPrice > newUnitPrices.min) previousInvoice.minUnitPrice else newUnitPrices.min
    val numberOfItems = newPurchases.map(purchase => purchase.quantity).sum + previousInvoice.numberItems
    val time = getHourFromDateTime(newPurchases.head.invoiceDate)
    val lastUpdated = if (previousInvoice.lastUpdated == 0.0) now else previousInvoice.lastUpdated
    val lines = newPurchases.length + previousInvoice.lines
    val average = (previousInvoice.avgUnitPrice * previousInvoice.lines + newUnitPrices.sum) / lines

    Invoice(invoiceNo, average, minimum, maximum, time, numberOfItems, lastUpdated, lines, customer)
  }

  /**
    * Given a purchases feed formatted as csv-like strings, returns those purchases parsed as objects by using a
    * csv parser
    *
    * @param purchase Feed from Kafka with purchases as csv-like strings
    * @return A purchase instance
    */
  def parsePurchase(purchase: String): Purchase = {
    //    val csvParserSettings = new CsvParserSettings()
    //    csvParserSettings.detectFormatAutomatically()
    val csvParser = new CsvParser(new CsvParserSettings())

    val parsedPurchase: Array[String] = csvParser.parseLine(purchase)

    parsedStringToPurchase(parsedPurchase)
  }

  /**
    * For the given invoice, model and threshold, it detects if it is an anomaly by using the distToCentroid function.
    * It predicts the distance to the assigned centroid for the trained model. If the distance is greater than the
    * given threshold, it is an anomaly.
    *
    * @param invoice   An invoice instance, to predict if is anomaly
    * @param model     A trained model (KMeans | BisectionKMeans)
    * @param threshold A calculated threshold
    * @return True if the invoice it is an anomaly
    */
  def isAnomaly(invoice: Invoice, model: Saveable, threshold: Double): Boolean = {
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

    distance.>(threshold)
  }

  /**
    * Given a invoice, checks if the quantity property is negative. If so, it is a cancellation
    *
    * @param invoice A invoice input
    * @return True if the invoice is a cancellation
    */
  def isCancellation(invoice: Invoice): Boolean = {
    invoice.invoiceNo.startsWith("C")
  }

  /**
    * Checks if the purchase has any missing or wrong property
    *
    * @param purchase A purchase input
    * @return True if the purchase have any wrong property
    */
  def isWrongPurchase(purchase: Purchase): Boolean = {
    purchase.invoiceNo == null || purchase.invoiceDate == null || purchase.customerID == null ||
      purchase.invoiceNo.isEmpty || purchase.invoiceDate.isEmpty || purchase.customerID.isEmpty ||
      purchase.unitPrice.isNaN || purchase.quantity.isNaN || purchase.country.isEmpty ||
      purchase.unitPrice.<(0)
  }

  /**
    * Given a Record object, returns a Purchase object
    *
    * @param parsedItems Record input (parseRecord output from CsvParser class)
    * @return A Purchase instance
    */
  private def parsedStringToPurchase(parsedItems: Array[String]) = Purchase(
    parsedItems(0),
    parsedItems(3).toInt,
    parsedItems(4),
    parsedItems(5).toDouble,
    parsedItems(6),
    parsedItems(7)
  )

  /**
    * Parses a dateTime string and gets the hour
    *
    * @param dateTime An date-time string with a format like dd/MM/YYYY hh:mm
    * @return Returns the hour related to the input string
    */
  def getHourFromDateTime(dateTime: String): Double = {
    var hour: Double = 0.0f

    val parsedDateTime = dateTime.substring(10).split(":")(0)
    if (!StringUtils.isEmpty(parsedDateTime))
      hour = parsedDateTime.trim.toDouble

    hour
  }
}
