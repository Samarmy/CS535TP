import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat

object SaveStream {
    def main(args: Array[String]) {
      //args(0) is the name of the machine where the streams are running
      //args(1) is the number of the user relations stream's socket port
      //args(2) is the number of the user hashtags stream's socket port
      //args(3) is the number of the user data stream's socket port
        val conf = new SparkConf()
        .setAppName("SaveStream")
        .set("spark.cores.max", "4")
        val ssc = new StreamingContext(conf, Seconds(60))
        val customDateFormat = new SimpleDateFormat("_MM.dd.yy_kk.mm.ss")

        def getDateTime: String = customDateFormat.format(Calendar.getInstance().getTime())
        def mostRecentUser(user1: Array[String], user2: Array[String]): Array[String] = if(user1(3).toDouble.toLong > user2(3).toDouble.toLong) user1 else user2
        def rddFormat(rdd: ReceiverInputDStream[String]): DStream[Array[String]] = rdd.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty)

        val userRelations = rddFormat(ssc.socketTextStream(args(0), args(1).toInt)).map(x => (x(0),x(1)))
        val userHashtags = rddFormat(ssc.socketTextStream(args(0), args(2).toInt)).map(x => (x(0),x.drop(1)))
        val userData = rddFormat(ssc.socketTextStream(args(0), args(3).toInt)).map(x => (x(0),x.drop(1)))

        userRelations.foreachRDD(s => {
          if(s.count() > 0){
            s.distinct()
            .repartition(1)
            .saveAsTextFile("hdfs:///LG/userRelations/userRelations" + getDateTime)
          }
        })

        userHashtags.foreachRDD(s => {
          if(s.count() > 0){
            s.distinct()
            .mapValues(x => x.mkString(" "))
            .repartition(1)
            .saveAsTextFile("hdfs:///LG/userHashtags/userHashtags" + getDateTime)
          }
        })

        userData.foreachRDD(s => {
          if(s.count() > 0){
            s.reduceByKey(mostRecentUser)
            .mapValues(x => x.mkString(" "))
            .repartition(1)
            .saveAsTextFile("hdfs:///LG/userData/userData" + getDateTime)
          }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
