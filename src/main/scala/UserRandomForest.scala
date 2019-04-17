import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.util.Try
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.feature.StringIndexer

case class hashtag(label:Int,user:String,quote_count:Int,reply_count:Int,retweet_count:Int,favorite_count:Int,favorited:Boolean,retweeted:Boolean,filter_level:String,followers_count:Int,friends_count:Int,listed_count:Int,favourites_count:Int,geo_enabled:Boolean,verified:Boolean,statuses_count:Int,contributors_enabled:Boolean,is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean,default_profile_image:Boolean)

object UserRandomForest {
    def main(args: Array[String]) {
      val spark = SparkSession.builder.appName("UserRandomForest").getOrCreate()
      import spark.implicits._
      val sc = spark.sparkContext
      val user = "Chowdhuri007"
      val ht = "politics"

      val userHashtags = spark.read.textFile("/tp/userHashtags*/*").rdd.map(x => {
        var strAry = x.split(",")
        (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
      }).mapValues(x => x.split(" ")).filter(x => x._2(0).equalsIgnoreCase(ht))
      //(digitaljournal,politics 0 0 0 0 False False low), (Reloaded_Kore,politics 0 0 0 0 False False low)
      val userRelations = spark.read.textFile("/tp/userRelations*/*").rdd.map(x => {
        var strAry = x.split(",")
        (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
      }).filter(x => x._1.equalsIgnoreCase(user)).map(x => x.swap).reduceByKey((v1, v2) => v1)
      val userRDD = sc.parallelize(Seq((user,user)))

      val users = userRelations ++ userRDD
      //(Chowdhuri007,MAMUNTK)
      val userData = spark.read.textFile("/tp/userData*/*").rdd.map(x => {
        var strAry = x.split(",")
        (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
      }).reduceByKey((v1, v2) => v1).mapValues(x => x.split(" ")).mapValues(x => x.slice(0,3) ++ x.slice(4,5) ++ x.slice(6,14)).join(users).map(x => (x._1, x._2._1))
      //(MAMUNTK,Array(61, 367, 0, 4, False, False, 20, False, False, True, True, False)

      val data =  userHashtags.join(userData).map(x => hashtag(1, x._1, x._2._1(1).toInt, x._2._1(2).toInt, x._2._1(3).toInt, x._2._1(4).toInt, Try(x._2._1(5).toBoolean).getOrElse(false), Try(x._2._1(6).toBoolean).getOrElse(false), x._2._1(7), x._2._2(0).toInt, x._2._2(1).toInt, x._2._2(2).toInt, x._2._2(3).toInt, Try(x._2._2(4).toBoolean).getOrElse(false), Try(x._2._2(5).toBoolean).getOrElse(false), x._2._2(6).toInt, Try(x._2._2(7).toBoolean).getOrElse(false), Try(x._2._2(8).toBoolean).getOrElse(false), Try(x._2._2(9).toBoolean).getOrElse(false), Try(x._2._2(10).toBoolean).getOrElse(false), Try(x._2._2(11).toBoolean).getOrElse(false))).toDF

      val indexer = new StringIndexer().setInputCol("filter_level").setOutputCol("filter_level_index").fit(data)

      val hasher = new FeatureHasher()
        hasher.setInputCols("user", "quote_count", "reply_count", "retweet_count", "favorite_count", "favorited", "retweeted", "filter_level_index", "followers_count", "friends_count", "listed_count", "favourites_count", "geo_enabled", "verified", "statuses_count", "contributors_enabled", "is_translator", "profile_use_background_image", "default_profile", "default_profile_image")
        hasher.setOutputCol("features")

      val Array(trainingData, testData) = data.randomSplit(Array(0.5, 0.5))

      val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")

      val pipeline = new Pipeline().setStages(Array(indexer, hasher, rf))

      val model = pipeline.fit(trainingData)

      val predictions = model.transform(testData)

      // Select example rows to display.
      // predictions.select("prediction", "label", "features").show

      // Select (prediction, true label) and compute test error.
      val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      // println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

      val rfModel = model.stages(2).asInstanceOf[RandomForestRegressionModel]
      // println(s"Learned regression forest model:\n ${rfModel.toDebugString}")
      // $example off$

      sc.parallelize(Seq(s"Root Mean Squared Error (RMSE) on test data = $rmse")).saveAsTextFile("/tp/rmse")
      sc.parallelize(Seq(s"Learned regression forest model:\n ${rfModel.toDebugString}")).saveAsTextFile("/tp/model")

      spark.stop()
    }
}
