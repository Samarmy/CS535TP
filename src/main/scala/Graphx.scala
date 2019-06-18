import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD
import scala.util.Try
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat
import org.apache.commons.io.FileUtils
import com.google.gson.Gson
import java.io.{InputStream, OutputStream, File}
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.util.concurrent.Executors;
import java.net.InetSocketAddress
import java.util.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.Try
import java.util.Date
import java.lang.System.currentTimeMillis
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.PipelineModel

class VertexProperty()
case class tweet(hashtag:String,screen_name:String,time:Long,quote_count:Int,reply_count:Int,retweet_count:Int,favorite_count:Int,favorited:Boolean,retweeted:Boolean,filter_level:String) extends VertexProperty
case class user(screen_name:String,followers_count:Int,friends_count:Int,listed_count:Int,time:Long,favourites_count:Int,verified:Boolean,statuses_count:Int,contributors_enabled:Boolean,default_profile:Boolean,default_profile_image:Boolean,labels:Map[String, Array[Long]],hashtags:Array[tweet],prediction:Map[String, Double]) extends VertexProperty
case class JsonHolder(j: String){
    var json = j
}
// case class hash_data(screen_name:String,label:Int,followers_count:Int,friends_count:Int,listed_count:Int,time_user:Long,favourites_count:Int,time_zone:String,geo_enabled:Boolean,verified:Boolean,statuses_count:Int, contributors_enabled:Boolean, is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean, default_profile_image:Boolean, following:Boolean,follow_request_sent:Boolean, notifications:Boolean, translator_type:String, num_tweets:Int, num_user_relations:Int) extends Serializable
  case class test_data(screen_name:String,followers_count:Int,friends_count:Int,listed_count:Int,time_user:Long,favourites_count:Int,time_zone:String,geo_enabled:Boolean,verified:Boolean,statuses_count:Int, contributors_enabled:Boolean, is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean, default_profile_image:Boolean, following:Boolean,follow_request_sent:Boolean, notifications:Boolean, translator_type:String, num_tweets:Int, num_user_relations:Int) extends Serializable

object Graphx {
    case class JsNode(id: Long, name: String, hashtags: HashMap[String,Array[Long]])
    case class JsLink(source: Int, target: Int)
    case class JsGraph(nodes: Array[JsNode], links: Array[JsLink], predictions: HashMap[String,Double])
    var jh = JsonHolder("")

    def main(args: Array[String]) {
        //args must be input for the following
        //args(0) is the name of the machine where the streams are running
        //args(1) is the number of the user relations stream's socket port
        //args(2) is the number of the user hashtags stream's socket port
        //args(3) is the number of the user data stream's socket port

        val spark = SparkSession.builder.appName("Graphx").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(60))

        def mostRecentUser(user1: Array[String], user2: Array[String]): Array[String] = if(user1(3).toDouble.toLong > user2(3).toDouble.toLong) user1 else user2
        def rddFormat(rdd: ReceiverInputDStream[String]): DStream[Array[String]] = rdd.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty)

        val userRelationsD = rddFormat(ssc.socketTextStream(args(0), args(1).toInt)).map(x => (x(0),x(1)))
        val userHashtagsD = rddFormat(ssc.socketTextStream(args(0), args(2).toInt)).map(x => (x(0),x.drop(1)))
        val userDataD = rddFormat(ssc.socketTextStream(args(0), args(3).toInt)).map(x => (x(0),x.drop(1)))

        userRelationsD.foreachRDD(s => {
          ssc.sparkContext.textFile("hdfs://austin:30121/LG/userRelations/*").map(x => {
            var strAry = x.split(",")
            (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
          }).union(s).distinct().repartition(10).saveAsTextFile("hdfs://austin:30121/LG/userRelations")
        })

        userHashtagsD.foreachRDD(s => {
        ssc.sparkContext.textFile("hdfs://austin:30121/LG/userHashtags/*").map(x => {
          var strAry = x.split(",")
          (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
        }).mapValues(x => x.split(" ")).union(s).distinct().mapValues(x => x.mkString(" ")).repartition(10).saveAsTextFile("hdfs://austin:30121/LG/userHashtags")
      })

        userDataD.foreachRDD(s => {
          ssc.sparkContext.textFile("hdfs://austin:30121/LG/userData/*").map(x => {
            var strAry = x.split(",")
            (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
          }).mapValues(x => x.split(" ")).union(s).reduceByKey(mostRecentUser).mapValues(x => x.mkString(" ")).repartition(10).saveAsTextFile("hdfs://austin:30121/LG/userData")
        })

        var userHashtags = spark.read.textFile("hdfs://austin:30121/LG/userHashtags/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1), tweet(props(0), strAry(0).substring(1), props(1).toLong,  props(2).toInt, props(3).toInt, props(4).toInt, props(5).toInt, Try(props(6).toBoolean).getOrElse(false), Try(props(7).toBoolean).getOrElse(false), props(8)))
        }).distinct().repartition(10).cache()

        val userData = spark.read.textFile("hdfs://austin:30121/LG/userData/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1), user(strAry(0).substring(1), props(0).toInt, props(1).toInt, props(2).toInt, props(3).split("\\.")(0).toLong, props(4).toInt, Try(props(5).toBoolean).getOrElse(false), props(6).toInt, Try(props(7).toBoolean).getOrElse(false), Try(props(8).toBoolean).getOrElse(false), Try(props(9).toBoolean).getOrElse(false), Map.empty[String, Array[Long]], Array[tweet](), Map.empty[String, Double]))
        }).reduceByKey((v1, v2) => {
          if(v1.time > v2.time){
            v1
          }else{
            v2
          }
        }).join(userHashtags).map(x => {
          var tweet_data = x._2._2
          var str = tweet_data.hashtag
          var lng = tweet_data.time
          // user_data.hashtags :+ tweet_data
          (x._1, x._2._1.copy(labels=Map(str -> Array(lng)), hashtags=Array(tweet_data)))
        }).reduceByKey((v1, v2) => v1.copy(labels=(v1.labels ++ v2.labels.map{ case (k,v) => k -> (v ++ v1.labels.getOrElse(k,Array[Long]()))}), hashtags = (v1.hashtags ++ v2.hashtags))).cache()

        val userRelations = spark.read.textFile("hdfs://austin:30121/LG/userRelations/*").rdd.map(x => {
          var strAry = x.split(",")
          (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
        }).cache()

        val userVertices = userData.zipWithUniqueId().map(x => (x._1._1, (x._2, x._1._2))).cache()

        val vertices: RDD[(VertexId, user)] = userVertices.map(x => x._2)

        val edges = userRelations.join(userVertices).map(x => (x._2._1, x._2._2._1)).join(userVertices).map(x => Edge(x._2._1, x._2._2._1, "follows")).cache()

        val defaultUser = user("unknown",0,0,0,0L,0,false,0,false,false,false,Map[String, Array[Long]](),Array[tweet](),Map[String, Double]())

        val graph = Graph(vertices, edges, defaultUser)

        val incomingHashMap = sc.broadcast(graph.collectNeighbors(EdgeDirection.Out).collectAsMap.toMap)

        /*val hashtag_ary = Array("politics", "funny", "win", "happybirthday", "metoo", "photography", "marvel", "pets", "friends", "science", "birthday", "tech", "technology", "fashion", "trump", "impeachdonaldtrump", "news", "fakenews", "family", "food", "summer", "usa", "love", "men", "women")

        def show(x: Option[Array[(org.apache.spark.graphx.VertexId, Any)]]) = x match {
              case Some(s) => s
              case None => Array[(org.apache.spark.graphx.VertexId, Any)]()
           }
        def show2(x: Option[Array[Long]]) = x match {
             case Some(s) => s
             case None => Array[Long]()
          }

        var beggining_of_data = 1555912800000L
        var lookback_days = 3L
        var data = sc.emptyRDD[hash_data].persist(MEMORY_AND_DISK_SER)
        var data2 = sc.emptyRDD[test_data].persist(MEMORY_AND_DISK_SER)
        for(hashtag_str <- hashtag_ary){
          var lookback_time = currentTimeMillis()
          var temp_data = vertices.filter(x => x._2.asInstanceOf[user].labels.getOrElse(hashtag_str, Array[Long]()).length > 0)
          while(lookback_time > 1555912800000L){
            var temp_data_2 = temp_data.filter(x => {
               var boo = false
               for(z <- x._2.asInstanceOf[user].labels.getOrElse(hashtag_str, Array[Long]())){
                 if(z < (lookback_time - 86400000L)){
                   boo = true
                 }
               }
              boo
            }).map(x => {
              var user1 = x._2.asInstanceOf[user]
              var ary = user1.hashtags
              var label = 0
              var num_tw = 0
              var num_rel = 0
              for(h <- ary){
                if(h.hashtag == hashtag_str){
                  if(h.time < lookback_time && h.time >= (lookback_time - 86400000L)){
                    label = label + 1
                  }else if(h.time < (lookback_time - 86400000L) && h.time > (lookback_time - (86400000L*(3L + 1L) ))){
                    num_tw = num_tw + 1
                  }
                }
              }
              for(u <- show(incomingHashMap.value.get(x._1))){
                for(h2 <- u._2.asInstanceOf[user].hashtags){
                  if(h2.hashtag == hashtag_str && h2.time < (lookback_time - 86400000L) && h2.time > (lookback_time - (86400000L*(3L + 1L) ))){
                      num_rel = num_rel + 1
                  }
                }
              }
              hash_data(user1.screen_name, label, user1.followers_count, user1.friends_count, user1.listed_count, user1.time, user1.favourites_count, user1.time_zone, user1.geo_enabled, user1.verified, user1.statuses_count, user1.contributors_enabled, user1.is_translator, user1.profile_use_background_image, user1.default_profile, user1.default_profile_image, user1.following, user1.follow_request_sent, user1.notifications, user1.translator_type, num_tw, num_rel)
            }).cache()
            data = data ++ temp_data_2.cache()
            //data.count()
            lookback_time = lookback_time - 86400000L
          }

          var temp_data_3 = temp_data.map(x => {
            var user1 = x._2.asInstanceOf[user]
            var ary = user1.hashtags
            var num_tw = 0
            var num_rel = 0
            for(h <- ary){
              if(h.hashtag == hashtag_str){
                if(h.time > (currentTimeMillis() - (86400000L*3L))){
                  num_tw = num_tw + 1
                }
              }
            }
            for(u <- show(incomingHashMap.value.get(x._1))){
              for(h2 <- u._2.asInstanceOf[user].hashtags){
                if(h2.hashtag == hashtag_str && h2.time > (currentTimeMillis() - (86400000L*(3L) ))){
                    num_rel = num_rel + 1
                }
              }
            }
            test_data(user1.screen_name, user1.followers_count, user1.friends_count, user1.listed_count, user1.time, user1.favourites_count, user1.time_zone, user1.geo_enabled, user1.verified, user1.statuses_count, user1.contributors_enabled, user1.is_translator, user1.profile_use_background_image, user1.default_profile, user1.default_profile_image, user1.following, user1.follow_request_sent, user1.notifications, user1.translator_type, num_tw, num_rel)
          }).cache()
          data2 = data2 ++ temp_data_3.cache()
        }
        data = data.repartition(10)
        var training = data.toDF
        var testing = data2.toDF
        val indexer1 = new StringIndexer().setInputCol("time_zone").setOutputCol("time_zone_index").fit(training)
        val indexer2 = new StringIndexer().setInputCol("translator_type").setOutputCol("translator_type_index").fit(training)

        val hasher = new FeatureHasher()
        hasher.setInputCols("followers_count", "friends_count", "listed_count", "time_user", "favourites_count", "time_zone_index", "geo_enabled", "verified", "statuses_count", "contributors_enabled", "is_translator", "profile_use_background_image", "default_profile", "default_profile_image", "following", "follow_request_sent", "notifications", "translator_type_index", "num_tweets", "num_user_relations")
        hasher.setOutputCol("features")

        val rf = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("features")

        val pipeline = new Pipeline().setStages(Array(indexer1, indexer2, hasher, rf))

        ///////This is for getting predictions for tomorrow
        val hashPredMap = new HashMap[String,Double]()
        //val model = pipeline.fit(training)

        val model = PipelineModel.load("hdfs://austin:30121/tp/pipelineModel")
        val predictions = model.transform(testing)
        */
        val predMap = predictions.map{ case p: test_data =>
                    (p(0).asInstanceOf[test_data].screen_name, p(1).asInstanceOf[Double])
                }.rdd.collectAsMap.toMap
        predMap.map{ case (k,v) =>
                        hashPredMap.put(k,v)
                    }

        val incoming = sc.broadcast(graph.collectNeighbors(EdgeDirection.Out).collectAsMap.toMap)
        val outgoing = sc.broadcast(graph.collectNeighbors(EdgeDirection.In).collectAsMap.toMap)

        val vizVerts = graph.vertices.flatMap{case (id, u: user) =>
                    val hashtags = new HashMap[String,Array[Long]]
                    val n1 = incoming.value.get(id)
                    val n2 = outgoing.value.get(id)
                    if (n1.get.length == 0 && n2.get.length == 0) { Array[JsNode]() } else {
                        u.labels.map{ case(k,v) => v.map{value =>
                                                        if (hashtags.containsKey(k)){
                                                            hashtags.put(k,hashtags.get(k) :+ value)
                                                        } else {
                                                            hashtags.put(k,Array[Long](value))
                                                        }
                                                    }}
                        Array[JsNode](JsNode(id, u.screen_name, hashtags))
                    }
                  }.distinct().collect()

        val vizEdges = graph.edges.map{
                    edge => JsLink(edge.srcId.toInt, edge.dstId.toInt)
                  }.collect()

        var gson = new Gson()
        jh.json = gson.toJson(JsGraph(vizVerts, vizEdges, hashPredMap))

        val server = HttpServer.create(new InetSocketAddress(11777), 0)
        server.createContext("/", new RootHandler(jh))
        server.setExecutor(null)
        server.start()

        //spark.stop()
        ssc.start()
        ssc.awaitTermination()
    }
}

class RootHandler(jh: JsonHolder) extends HttpHandler {

  def handle(t: HttpExchange) {
    displayPayload(t.getRequestBody)
    sendResponse(t)
  }

  private def displayPayload(body: InputStream): Unit ={
    println()
    println("******************** REQUEST START ********************")
    println()
    copyStream(body, System.out)
    println()
    println("********************* REQUEST END *********************")
    println()
  }

  private def copyStream(in: InputStream, out: OutputStream) {
    Iterator
      .continually(in.read)
      .takeWhile(-1 !=)
      .foreach(out.write)
  }

  private def sendResponse(t: HttpExchange) {
    //FileUtils.writeStringToFile(new File("/s/chopin/a/grad/kevincb/test.json"), json)
    t.getResponseHeaders().add("Content-Type", "application/json");
    //t.getResponseHeaders().add("Content-Encoding", "gzip");
    t.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    t.getResponseHeaders().add("Access-Control-Allow-Methods", "POST");
    t.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type,Content-Encoding");
    t.sendResponseHeaders(200, jh.json.length())
    val os = t.getResponseBody
    os.write(jh.json.getBytes)
    os.close()
  }
}
