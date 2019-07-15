import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD
import  org.apache.spark.graphx.VertexRDD

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.io.{InputStream, OutputStream, File}
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress
import scala.io.Source

object SimplificationDegree {
    var numLevels = 10
    var numNodes = 100

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

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
        if (a._2 > b._2) a else b
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("SimplificationDegree").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        val originalGraph = graphx.util.GraphGenerators.logNormalGraph(sc, numNodes, 0, 1.0, 1.0)
        numLevels = originalGraph.degrees.reduce(max)._2
        val graph = originalGraph.mapVertices((id, _) => (0, 0, Array.fill[Long](numLevels)(id)))
       
        var degreeGraph = graph.outerJoinVertices(graph.outDegrees)((id, oldAttr, outDegOpt) => (outDegOpt.getOrElse(0), outDegOpt.getOrElse(0), oldAttr._3)).cache()
        var degreeGraphVertices = degreeGraph.vertices.cache()
        val degreeZeroVertices = degreeGraphVertices.filter{
          case (id, x) =>  x._1 == 0
        }

        for (x <- (numLevels - 1) to 1 by -1) {
          degreeGraphVertices = degreeGraph.aggregateMessages[(Int, Int, Array[Long])](
            triplet => {  // Send Message
              if(triplet != null && triplet.dstAttr != null && triplet.srcAttr != null){
                if(triplet.dstAttr._1 <= x && triplet.dstAttr._1 < triplet.srcAttr._1){
                  triplet.sendToDst(triplet.srcAttr)
                  triplet.sendToDst(triplet.dstAttr)
                }else{
                  triplet.sendToSrc(triplet.srcAttr)
                  triplet.sendToDst(triplet.dstAttr)
                }
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
            degreeGraph = Graph(degreeGraphVertices, graph.edges).cache()
          }
        }

        val finalGraph = Graph(degreeGraphVertices.mapValues(x => x._3) ++ degreeZeroVertices.mapValues(x => x._3), graph.edges).cache()

        var gh = GraphHolder(finalGraph)
        val server = HttpServer.create(new InetSocketAddress(11777), 0)
        server.createContext("/", new ResponseHandler(gh))
        server.setExecutor(null)
        server.start()
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
