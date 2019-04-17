import org.apache.spark._
import org.apache.spark.streaming._
import java.util.{Calendar, Date}

object StreamingExampleWithHadoop {

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("StreamingExampleWithHadoop")
        val ssc = new StreamingContext(conf, Seconds(10))
        // val sc = new SparkContext(conf)

        var time = System.currentTimeMillis().toString
        val userRelationLines = ssc.socketTextStream("madison", 11711)
        val userRelations = userRelationLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x(1)))
        userRelations.foreachRDD(s => s.saveAsTextFile("hdfs:///tp/userRelations" + System.currentTimeMillis().toString))
        // userRelations consists of pairs of (screen_name, follower_screen_name)
        val userHashtagLines = ssc.socketTextStream("madison", 11712)
        val userHashtags = userHashtagLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x.drop(1))).mapValues(x => x.mkString(" "))
        userHashtags.foreachRDD(s => s.saveAsTextFile("hdfs:///tp/userHashtags" + System.currentTimeMillis().toString))
        // userHashtags consists of pairs of (screen_name, hashtag)
        val userDataLines = ssc.socketTextStream("madison", 11713)
        val userData = userDataLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x.drop(1))).mapValues(x => x.mkString(" "))
        userData.foreachRDD(s => s.saveAsTextFile("hdfs:///tp/userData" + System.currentTimeMillis().toString))
        // userHashtags consists of pairs of (screen_name, user_data_array) - look at the python script to see what is present in the array
        ssc.start()
        ssc.awaitTermination()
    }
}
