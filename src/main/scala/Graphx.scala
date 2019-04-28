import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Try

class VertexProperty()
case class tweet(hashtag:String,screen_name:String,time:Long,quote_count:Int,reply_count:Int,retweet_count:Int,favorite_count:Int,favorited:Boolean,retweeted:Boolean,filter_level:String) extends VertexProperty
case class user(screen_name:String,followers_count:Int,friends_count:Int,listed_count:Int,time:Long,favourites_count:Int,time_zone:String,geo_enabled:Boolean,verified:Boolean,statuses_count:Int,contributors_enabled:Boolean,is_translator:Boolean,profile_use_background_image:Boolean,default_profile:Boolean,default_profile_image:Boolean,following:Boolean,follow_request_sent:Boolean,notifications:Boolean,translator_type:String) extends VertexProperty

object Graphx {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Graphx").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        val userHashtags = spark.read.textFile("/test/userHashtags/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1) + " t", tweet(props(0), strAry(0).substring(1), props(1).toLong,  props(2).toInt, props(3).toInt, props(4).toInt, props(5).toInt, Try(props(6).toBoolean).getOrElse(false), Try(props(7).toBoolean).getOrElse(false), props(8)).asInstanceOf[Any])
        }).cache()
        //(maxgin42,happybirthday 1555969426304 0 0 0 0 False False low)
        val userData = spark.read.textFile("/test/userData/*").rdd.map(x => {
          var strAry = x.split(",")
          var props = strAry(1).substring(0, strAry(1).length - 1).split(" ")
          (strAry(0).substring(1) + " u", user(strAry(0).substring(1), props(0).toInt, props(1).toInt, props(2).toInt, props(3).split("\\.")(0).toLong, props(4).toInt, props(5), Try(props(6).toBoolean).getOrElse(false), Try(props(7).toBoolean).getOrElse(false), props(8).toInt, Try(props(9).toBoolean).getOrElse(false), Try(props(10).toBoolean).getOrElse(false), Try(props(11).toBoolean).getOrElse(false), Try(props(12).toBoolean).getOrElse(false), Try(props(13).toBoolean).getOrElse(false), Try(props(14).toBoolean).getOrElse(false), Try(props(15).toBoolean).getOrElse(false), Try(props(16).toBoolean).getOrElse(false), props(17)).asInstanceOf[Any])
        }).cache()
        //(LeadingWPassion,50495 49713 282 1445416821000.0 15299 None False False 434339 False False True False False None None None none)
        val userRelations = spark.read.textFile("/test/userRelations/*").rdd.map(x => {
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

        spark.stop()
    }
}
