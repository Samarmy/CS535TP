import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.io.{InputStream, OutputStream, File}
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress
import scala.io.Source

object SimplificationDegree {
    val numLevels = 4

    case class JsNode(id: Double, realID: Long)
    case class JsLink(source: Int, target: Int)
    case class JsGraph(nodes: Array[JsNode], links: Array[JsLink])

    def getLevel(a: Array[Long]): Int = {
      var level = 1
      for (x <- 0 to a.size - 2){
        if(a(x) != a(x+1)){
          level = level + 1
        }
      }
      return level
    }

    def merge(a: Array[Long], b: Array[Long]) : Array[Long] = {
        var n = getLevel(a)
        var retArray = b.clone
        if(n <= retArray.size){
          for ( x <- 0 to n - 1) retArray(x) = a(x)
        }
        return retArray
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("SimplificationDegree").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        val vertices: RDD[(VertexId, (Int, Int, Array[Long]))] =
          sc.parallelize(Array((1L, (0, 0, Array.fill[Long](numLevels)(1L))),
                               (2L, (0, 0, Array.fill[Long](numLevels)(2L))),
                               (3L, (0, 0, Array.fill[Long](numLevels)(3L))),
                               (4L, (0, 0, Array.fill[Long](numLevels)(4L))),
                               (5L, (0, 0, Array.fill[Long](numLevels)(5L))),
                               (6L, (0, 0, Array.fill[Long](numLevels)(6L))),
                               (7L, (0, 0, Array.fill[Long](numLevels)(7L))),
                               (8L, (0, 0, Array.fill[Long](numLevels)(8L)))))

        val edges: RDD[Edge[Int]] =
          sc.parallelize(Array(Edge(1L, 2L, 1), Edge(1L, 3L, 1), Edge(1L, 4L, 1), Edge(1L, 5L, 1),
                               Edge(2L, 1L, 1), Edge(2L, 3L, 1), Edge(2L, 4L, 1), Edge(2L, 5L, 1),
                               Edge(3L, 1L, 1), Edge(3L, 2L, 1), Edge(3L, 4L, 1), Edge(3L, 6L, 1),
                               Edge(4L, 1L, 1), Edge(4L, 2L, 1), Edge(4L, 3L, 1), Edge(4L, 6L, 1),
                               Edge(5L, 1L, 1), Edge(5L, 2L, 1),
                               Edge(6L, 3L, 1), Edge(6L, 4L, 1), Edge(6L, 7L, 1),
                               Edge(7L, 6L, 1), Edge(7L, 8L, 1),
                               Edge(8L, 7L, 1)
                             ))

        val defaultVertex = (0, 0, Array.fill[Long](numLevels)(-1L))

        // Build the initial Graph
        val graph = Graph(vertices, edges, defaultVertex)
        var newGraph = graph.outerJoinVertices(graph.outDegrees)((id, oldAttr, outDegOpt) => (outDegOpt.getOrElse(0), outDegOpt.getOrElse(0), oldAttr._3)).cache()
        var clusteredGraph = newGraph.vertices.cache()

        for (x <- (numLevels - 1) to 1 by -1) {
          clusteredGraph = newGraph.aggregateMessages[(Int, Int, Array[Long])](
            triplet => {  // Send Message
              if(triplet.dstAttr._1 <= x && triplet.dstAttr._1 < triplet.srcAttr._1){
                triplet.sendToDst(triplet.srcAttr)
                triplet.sendToDst(triplet.dstAttr)
              }else if (triplet.dstAttr._1 > x){
                triplet.sendToDst(triplet.dstAttr)
              }
            },
            (a,b) => {
              if(a._1 >= b._2){
                (b._1, a._1, merge(a._3, b._3))
              }else{
                b
              }
            }
          ).cache()
          if(x > 1){
            newGraph = Graph(clusteredGraph, graph.edges).cache()
          }
        }

        clusteredGraph.mapValues(x => "(" + x._1 + "," + x._2 + ",(" + x._3.mkString(",") + "))").saveAsTextFile("/test")
        val finalClusteredGraph = clusteredGraph.mapValues(x => x._3)
        val finalGraph = Graph(finalClusteredGraph, graph.edges).cache()

        var gh = GraphHolder(finalGraph)
        val server = HttpServer.create(new InetSocketAddress(11777), 0)
        server.createContext("/", new ResponseHandler(gh))
        server.setExecutor(null)
        server.start()

        //spark.stop()
    }

    class ResponseHandler(gh: GraphHolder) extends HttpHandler {

        def handle(t: HttpExchange) {
            val body = Source.fromInputStream(t.getRequestBody).mkString
            displayPayload(body)
            sendResponse(t, body)
        }

        private def displayPayload(body: String): Unit ={
            println()
            println("******************** REQUEST START ********************")
            println()
            println(body)
            println()
            println("********************* REQUEST END *********************")
            println()
        }

        private def makeResponse(body: String): String ={
            var gson = new Gson()
            val request = body.split(",").filter(_!="").map(_.toInt)

            val vizVerts = gh.graph.vertices.flatMap{case (id, prop) =>
                if (request.length == 0){
                    Array[JsNode](JsNode(prop(0), id))
                } else {
                    if(prop.take(request.length).sameElements(request)){
                        if (request.length == numLevels){
                            Array[JsNode](JsNode(id, id))
                        } else {
                            Array[JsNode](JsNode(prop(request.length), id))
                        }
                    } else {
                        Array[JsNode]()
                    }
                }
            }.distinct().collect()

            val vizEdges = gh.graph.edges.map{
                edge => JsLink(edge.srcId.toInt, edge.dstId.toInt)
            }.collect()

            return gson.toJson(JsGraph(vizVerts, vizEdges))
        }

        private def sendResponse(t: HttpExchange, body: String) {
            //FileUtils.writeStringToFile(new File("/s/chopin/a/grad/kevincb/test.json"), json)
            t.getResponseHeaders().add("Content-Type", "application/json");
            //t.getResponseHeaders().add("Content-Encoding", "gzip");
            t.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            t.getResponseHeaders().add("Access-Control-Allow-Methods", "POST");
            t.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type,Content-Encoding");
            var response = ""
            try {
                response = makeResponse(body)
            } catch {
                case e: Throwable => e.printStackTrace()
            }
            t.sendResponseHeaders(200, response.length())
            val os = t.getResponseBody
            os.write(response.getBytes)
            os.close()
        }
    }

    case class GraphHolder(g: Graph[Array[Long],Int]){
        var graph = g
    }
}
