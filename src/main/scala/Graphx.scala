import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Try
import java.util.Date
import org.apache.commons.io.FileUtils
import com.google.gson.Gson
import java.io.{InputStream, OutputStream, File}
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.util.concurrent.Executors;
import java.net.InetSocketAddress


class VertexProperty()
case class tweet(hashtag:String,screen_name:String,time:Long,quote_count:Int,reply_count:Int,retweet_count:Int,favorite_count:Int,favorited:Boolean,retweeted:Boolean,filter_level:String) extends VertexProperty
case class user(screen_name:String,followers_count:Int,friends_count:Int,listed_count:Int,time:Long,favourites_count:Int,time_zone:String,geo_enabled:Boolean,verified:Boolean,statuses_count:Int,contributors_enabled:Boolean,is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean,default_profile_image:Boolean,following:Boolean,follow_request_sent:Boolean,notifications:Boolean,translator_type:String,labels:Map[String, Array[Long]],hashtags:Array[tweet],prediction:Double) extends VertexProperty
case class JsonHolder(j: String){
    var json = j
}

object Graphx {
    case class JsNode(name: String, hashtags: String)
    case class JsLink(source: Int, target: Int)
    case class JsGraph(nodes: Array[JsNode], links: Array[JsLink])
    var jh = JsonHolder("")
    
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Graphx").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        var userHashtags = spark.read.textFile("hdfs://austin:30121/test/userHashtags/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1) + " t", tweet(props(0), strAry(0).substring(1), props(1).toLong,  props(2).toInt, props(3).toInt, props(4).toInt, props(5).toInt, Try(props(6).toBoolean).getOrElse(false), Try(props(7).toBoolean).getOrElse(false), props(8)).asInstanceOf[Any])
        }).cache()
        //(maxgin42,happybirthday 1555969426304 0 0 0 0 False False low)
        val userData = spark.read.textFile("hdfs://austin:30121/test/userData/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1) + " u", user(strAry(0).substring(1), props(0).toInt, props(1).toInt, props(2).toInt, props(3).split("\\.")(0).toLong, props(4).toInt, props(5), Try(props(6).toBoolean).getOrElse(false), Try(props(7).toBoolean).getOrElse(false), props(8).toInt, Try(props(9).toBoolean).getOrElse(false), Try(props(10).toBoolean).getOrElse(false), Try(props(11).toBoolean).getOrElse(false), Try(props(12).toBoolean).getOrElse(false), Try(props(13).toBoolean).getOrElse(false), Try(props(14).toBoolean).getOrElse(false), Try(props(15).toBoolean).getOrElse(false), Try(props(16).toBoolean).getOrElse(false), props(17), Map.empty[String, Array[Long]], Array[tweet](), 0.0))
        }).reduceByKey((v1, v2) => {
          if(v1.asInstanceOf[user].time > v2.asInstanceOf[user].time){
            v1
          }else{
            v2
          }
        }).join(userHashtags).map(x => {
          var tweet_data = x._2._2.asInstanceOf[tweet]
          var str = tweet_data.hashtag
          var lng = tweet_data.time
          // user_data.hashtags :+ tweet_data
          (x._1, x._2._1.asInstanceOf[user].copy(labels=Map(str -> Array(lng)), hashtags=Array(tweet_data)).asInstanceOf[Any])
        }).reduceByKey((v1, v2) => v1.asInstanceOf[user].copy(labels=(v1.asInstanceOf[user].labels ++ v2.asInstanceOf[user].labels), hashtags = (v1.asInstanceOf[user].hashtags ++ v2.asInstanceOf[user].hashtags))).map(x => (x._1 + " u", x._2)).cache()
        
        userHashtags = userHashtags.map(x => (x._1 + " t", x._2))
        //(LeadingWPassion,50495 49713 282 1445416821000.0 15299 None False False 434339 False False True False False None None None none)
        val userRelations = spark.read.textFile("hdfs://austin:30121/test/userRelations/*").rdd.map(x => {
          var strAry = x.split(",")
          (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
        }).cache()

        val indexedData = userData.union(userHashtags).zipWithUniqueId().cache()

        val userVertices = indexedData.filter(x => x._1._1.contains(" u")).map(x => {
          val userName = x._1._1
          (userName.substring(0, userName.length - 2), (x._2, x._1._2))
        }).cache()

        val tweetVertices = indexedData.filter(x => x._1._1.contains(" t")).map(x => {
          val userName = x._1._1
          (userName.substring(0, userName.length - 2), (x._2, x._1._2))
        }).cache()

        val userEdges = userRelations.join(userVertices).map(x => (x._2._1, x._2._2._1)).join(userVertices).map(x => Edge(x._2._1, x._2._2._1, "follows")).cache()

        val tweetEdges = tweetVertices.join(userVertices).map(x => Edge(x._2._2._1, x._2._1._1, "tweeted")).distinct().cache()

        val vertices: RDD[(VertexId, Any)] = tweetVertices.map(x => x._2).union(userVertices.map(x => x._2))

        val edges = userEdges ++ tweetEdges

        val blank = "None"

        val defaultUser = (blank.asInstanceOf[Any])

        val graph = Graph(vertices, edges, defaultUser)
    
        val vizVerts = graph.vertices.filter{
                    case (id, u: user) => true
                    case (id, t: tweet) => false
                    }.map{
                    case (id, u: user) => JsNode(u.screen_name, "0")
                  }.distinct().collect()
      
        val vizEdges = graph.edges.map{
                    edge => JsLink(edge.srcId.toInt, edge.dstId.toInt)
                  }.collect()
                  
        var gson = new Gson()
        jh.json = gson.toJson(JsGraph(vizVerts, vizEdges))    
      
        val server = HttpServer.create(new InetSocketAddress(11777), 0)
        server.createContext("/", new RootHandler(jh))
        server.setExecutor(null)
        server.start()
      
        //spark.stop()
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
