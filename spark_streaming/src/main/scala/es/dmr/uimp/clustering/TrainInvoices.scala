package es.dmr.uimp.clustering

import es.dmr.uimp.clustering.Clustering.elbowSelection
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

object TrainInvoices {

  val K_MEANS_MODEL = "kMeans"
  val BISECTION_K_MEANS_MODEL = "BisKMeans"
  val BASE_APP_NAME = "ClusterInvoices_"

  def main(args: Array[String]) {

    import Clustering._

    if (args.length < 4) {
      System.err.println("Required parameters: <csv-source-file> <model-storage-route> <threshold-storage-route> " +
        "<model-type-name: kMeans | BisKMeans>")
      System.exit(1)
    }

    val Array(trainDataRoute, modelStorageRoute, thresholdsStorageRoute, modelType) = args
    val appName = BASE_APP_NAME + modelType

    val sparkConfiguration = new SparkConf().setAppName(appName)
    //.setMaster(masterUrl)
    val sparkContext = new SparkContext(sparkConfiguration)

    val inputData = loadData(sparkContext, trainDataRoute)

    val dataSet = buildDataSet(inputData)
    dataSet.cache()
    dataSet.take(30).foreach(println)

    // Train and get distances between centroids
    val distances: RDD[Double] = if (modelType.eq(K_MEANS_MODEL)) {
      val model: KMeansModel = trainKMeansModel(dataSet)
      model.save(sparkContext, modelStorageRoute)

      dataSet.map(d => distToCentroidFromKMeans(d, model))
    } else if (modelType.eq(BISECTION_K_MEANS_MODEL)) {
      val model: BisectingKMeansModel = trainBisectingKMeansModel(dataSet)
      model.save(sparkContext, modelStorageRoute)

      dataSet.map(d => distToCentroidFromBisectingKMeans(d, model))
    } else {
      System.err.println("Invalid model name. Required types: kMeans, BisKMeans")
      return System.exit(1)
    }

    val threshold = distances.top(2000).last // set the last of the furthest 2000 data points as the threshold
    saveThreshold(threshold, thresholdsStorageRoute)
  }

  /**
    * Trains model using k-means method
    *
    * @param data invoice featured data
    * @return selected model for a chosen k by the elbow method
    */
  def trainKMeansModel(data: RDD[Vector]): KMeansModel = {

    val models = 1 to 20 map { k =>
      val kMeans = new KMeans()
      kMeans.setK(k) // find that one center
      kMeans.run(data)
    }

    val costs = models.map(model => model.computeCost(data))

    val selected = elbowSelection(costs, 0.7)
    System.out.println("Selecting k-means model: " + models(selected).k)
    models(selected)
  }

  /**
    * Trains model using Bisection k-means method
    *
    * @param data invoice featured data
    * @return selected model for a chosen k by the elbow method
    */
  def trainBisectingKMeansModel(data: RDD[Vector]): BisectingKMeansModel = {

    val models = 1 to 20 map { k =>
      val bisectionKMeans = new BisectingKMeans()
      bisectionKMeans.setK(k) // find that one center
      bisectionKMeans.run(data)
    }

    val costs = models.map(model => model.computeCost(data))

    val selected = elbowSelection(costs, 0.7)
    System.out.println("Selecting Bisection k-means model: " + models(selected).k)
    models(selected)
  }

  /**
    * Calculates distance between data point to centroid for a given KMeansModel
    *
    * @param datum
    * @param model
    * @return
    */
  def distToCentroidFromKMeans(datum: Vector, model: KMeansModel): Double = {
    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }

  /**
    * Calculates distance between data point to centroid for a given BisectingKMeansModel
    *
    * @param datum
    * @param model
    * @return
    */
  def distToCentroidFromBisectingKMeans(datum: Vector, model: BisectingKMeansModel): Double = {
    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }

  /**
    * Creates a data set (RDD) with features: mean, max, min, hour, numItems; from input data
    *
    * @param inputData : data frame with the parsed CSV data
    * @return
    */
  def buildDataSet(inputData: DataFrame): RDD[Vector] = {
    import Clustering._

    val filteredData = filterData(inputData)

    val featuredData = createFeaturesFromData(filteredData)

    val dataSet = convertToDataSet(featuredData)

    dataSet
  }
}

