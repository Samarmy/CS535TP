import org.apache.spark._
import org.apache.spark.streaming._
import java.util.{Calendar, Date}
import org.apache.hadoop.fs.{FileSystem, Path}

object StreamingExampleWithHadoop {

    def main(args: Array[String]) {
        //args must be input for the following
        //args(0) is the name of the machine where the streams are running
        //args(1) is the number of the user relations stream's socket port
        //args(2) is the number of the user hashtags stream's socket port
        //args(3) is the number of the user data stream's socket port
        val conf = new SparkConf().setAppName("StreamingExampleWithHadoop")
        val ssc = new StreamingContext(conf, Seconds(60))
        val fs = FileSystem.get(ssc.sparkContext.hadoopConfiguration)

        def mostRecentUser(user1: Array[String], user2: Array[String]): Array[String] = if(user1(3).toDouble.toLong > user2(3).toDouble.toLong) user1 else user2
        def rddFormat(rdd: ReceiverInputDStream[String]): DStream[Array[String]] = rdd.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty)

        val userRelationsD = rddFormat(ssc.socketTextStream(args(0), args(1).toInt)).map(x => (x(0),x(1)))
        val userHashtagsD = rddFormat(ssc.socketTextStream(args(0), args(2).toInt)).map(x => (x(0),x.drop(1)))
        val userDataD = rddFormat(ssc.socketTextStream(args(0), args(3).toInt)).map(x => (x(0),x.drop(1)))

        userRelationsD.foreachRDD(s => {
          if(fs.exists(new Path("/LG/userRelations"))){
            var userRelations = ssc.sparkContext.textFile("hdfs://austin:30121/LG/userRelations/*").map(x => {
              var strAry = x.split(",")
              (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
            }).union(s).distinct().repartition(10).cache()
            if(userRelations.count() > 0){
              userRelations.saveAsTextFile("hdfs://austin:30121/LG/userRelations")
            }
          }else{
            if(s.count() > 0){
              s.distinct().saveAsTextFile("hdfs://austin:30121/LG/userRelations")
            }
          }
        })

        userHashtagsD.foreachRDD(s => {
          if(fs.exists(new Path("/LG/userHashtags"))){
            var userHashtags = ssc.sparkContext.textFile("hdfs://austin:30121/LG/userHashtags/*").map(x => {
              var strAry = x.split(",")
              (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
            }).mapValues(x => x.split(" ")).union(s).distinct().mapValues(x => x.mkString(" ")).repartition(10).cache()
            if(userHashtags.count() > 0){
              userHashtags.saveAsTextFile("hdfs://austin:30121/LG/userHashtags")
            }
          }else{
            if(s.count() > 0){
              s.distinct().mapValues(x => x.mkString(" ")).saveAsTextFile("dfs://austin:30121/LG/userHashtags")
            }
          }
        })

        userDataD.foreachRDD(s => {
          if(fs.exists(new Path("/LG/userData"))){
            var userData = ssc.sparkContext.textFile("hdfs://austin:30121/LG/userData/*").map(x => {
              var strAry = x.split(",")
              (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
            }).mapValues(x => x.split(" ")).union(s).reduceByKey(mostRecentUser).mapValues(x => x.mkString(" ")).repartition(10).cache()
            if(userData.count() > 0){
              userData.saveAsTextFile("hdfs://austin:30121/LG/userData")
            }
          }else{
            if(s.count() > 0){
              s.reduceByKey(mostRecentUser).mapValues(x => x.mkString(" ")).saveAsTextFile("hdfs://austin:30121/LG/userData")
            }
          }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
