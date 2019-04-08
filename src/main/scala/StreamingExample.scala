import org.apache.spark._
import org.apache.spark.streaming._

object StreamingExample {

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("StreamingExample")
        val ssc = new StreamingContext(conf, Seconds(10))
        
        val userRelationLines = ssc.socketTextStream("tallahassee", 11711)
        val userRelations = userRelationLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x(1)))
		// userRelations consists of pairs of (screen_name, follower_screen_name)
        
        val userHashtagLines = ssc.socketTextStream("tallahassee", 11712)
        val userHashtags = userHashtagLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x(1)))
        // userHashtags consists of pairs of (screen_name, hashtag)
        
        val userDataLines = ssc.socketTextStream("tallahassee", 11713)
        val userData = userDataLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x.drop(1)))
        // userHashtags consists of pairs of (screen_name, user_data_array) - look at the python script to see what is present in the array
        
        ssc.start()
        ssc.awaitTermination()
    }
}
