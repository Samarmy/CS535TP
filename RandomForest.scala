import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RandomForest {
    def main(args: Array[String]) {
      val spark = SparkSession.builder.appName("RandomForest").getOrCreate()
      val sc = spark.sparkContext
      val data = spark.read.format("libsvm").load("/tp/sample_libsvm_data.txt")

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(data)

      // Split the data into training and test sets (30% held out for testing).
      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

      // Train a RandomForest model.
      val rf = new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures")

      // Chain indexer and forest in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, rf))

      // Train model. This also runs the indexer.
      val model = pipeline.fit(trainingData)

      // Make predictions.
      val predictions = model.transform(testData)

      // Select example rows to display.
      predictions.select("prediction", "label", "features").show(5)

      // Select (prediction, true label) and compute test error.
      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      // println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

      val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
      // println(s"Learned regression forest model:\n ${rfModel.toDebugString}")
      // $example off$

      sc.parallelize(Seq(s"Root Mean Squared Error (RMSE) on test data = $rmse")).saveAsTextFile("/tp/rmse")
      sc.parallelize(Seq(s"Learned regression forest model:\n ${rfModel.toDebugString}")).saveAsTextFile("/tp/model")

      spark.stop()
    }
}
