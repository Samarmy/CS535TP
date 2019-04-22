import org.apache.spark._
import org.apache.spark.streaming._
import java.util.{Calendar, Date}
import org.apache.hadoop.fs.{FileSystem, Path}

object StreamingExampleWithHadoop {

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("StreamingExampleWithHadoop")
        val ssc = new StreamingContext(conf, Seconds(60))
        val fs = FileSystem.get(ssc.sparkContext.hadoopConfiguration)

        val newUserRelationLines = ssc.socketTextStream("madison", 11711)
        val newUserHashtagLines = ssc.socketTextStream("madison", 11712)
        val newUserDataLines = ssc.socketTextStream("madison", 11713)
        val newUserRelations = newUserRelationLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x(1)))
        val newUserHashtags = newUserHashtagLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x.drop(1)))
        val newUserDatas = newUserDataLines.filter(_.nonEmpty).flatMap(_.split(":")).map(_.split(",")).filter(!_.isEmpty).map(x => (x(0),x.drop(1)))

        newUserRelations.foreachRDD(s => {
          if(fs.exists(new Path("/tp/userRelations"))){
            var userRelations = ssc.sparkContext.textFile("/tp/userRelations/*").map(x => {
              var strAry = x.split(",")
              (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
            }).union(s).distinct().repartition(10).cache()
            if(userRelations.count() > 0){
              userRelations.saveAsTextFile("hdfs:///tp/userRelations")
            }
          }else{
            if(s.count() > 0){
              s.distinct().saveAsTextFile("hdfs:///tp/userRelations")
            }
          }
        })

        newUserHashtags.foreachRDD(s => {
          if(fs.exists(new Path("/tp/userHashtags"))){
            var userHashtags = ssc.sparkContext.textFile("/tp/userHashtags/*").map(x => {
              var strAry = x.split(",")
              (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
            }).mapValues(x => x.split(" ")).union(s).distinct().mapValues(x => x.mkString(" ")).repartition(10).cache()
            if(userHashtags.count() > 0){
              userHashtags.saveAsTextFile("hdfs:///tp/userHashtags")
            }
          }else{
            if(s.count() > 0){
              s.distinct().mapValues(x => x.mkString(" ")).saveAsTextFile("hdfs:///tp/userHashtags")
            }
          }
        })

        newUserDatas.foreachRDD(s => {
          if(fs.exists(new Path("/tp/userData"))){
            var userData = ssc.sparkContext.textFile("/tp/userData/*").map(x => {
              var strAry = x.split(",")
              (strAry(0).substring(1), strAry(1).substring(0, strAry(1).length - 1))
            }).mapValues(x => x.split(" ")).union(s).reduceByKey((v1, v2) => {
              if(v1(3).toDouble.toLong > v2(3).toDouble.toLong){
                v1
              }else{
                v2
              }
            }).mapValues(x => x.mkString(" ")).repartition(10).cache()
            if(userData.count() > 0){
              userData.saveAsTextFile("hdfs:///tp/userData")
            }
          }else{
            if(s.count() > 0){
              s.reduceByKey((v1, v2) => {
                if(v1(3).toDouble.toLong > v2(3).toDouble.toLong){
                  v1
                }else{
                  v2
                }
              }).mapValues(x => x.mkString(" ")).saveAsTextFile("hdfs:///tp/userData")
            }
          }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
