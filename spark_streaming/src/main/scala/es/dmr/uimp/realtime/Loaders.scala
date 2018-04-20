package es.dmr.uimp.realtime

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{BisectingKMeansModel, KMeansModel}

object Loaders {

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

}
