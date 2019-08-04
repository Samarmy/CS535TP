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
        var retArray = b
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

        numLevels = args(0).toInt
        // val originalGraph = graphx.util.GraphGenerators.logNormalGraph(sc, numNodes, 0, 1.0, 1.0)

        var edges = sc.textFile("/socialNet/*").map(x => {
          var ary = x.split("\\s+")
            Edge(ary(0).toLong, ary(1).toLong, 1)
        })
        
        var graph = Graph.fromEdges(edges, 0L).mapVertices((id, _) => (0, 0, Array.fill[Long](numLevels)(id)))

        // val vertices: RDD[(VertexId, Long)] =
        //   sc.parallelize(Array((1L, 0L),
        //                        (2L, 0L),
        //                        (3L, 0L),
        //                        (4L, 0L),
        //                        (5L, 0L),
        //                        (6L, 0L),
        //                        (7L, 0L),
        //                        (8L, 0L)
        //                      ))
        //
        // val edges: RDD[Edge[Int]] =
        //   sc.parallelize(Array(Edge(1L, 2L, 1), Edge(1L, 3L, 1), Edge(1L, 4L, 1), Edge(1L, 5L, 1),
        //                        Edge(2L, 1L, 1), Edge(2L, 3L, 1), Edge(2L, 4L, 1), Edge(2L, 5L, 1),
        //                        Edge(3L, 1L, 1), Edge(3L, 2L, 1), Edge(3L, 4L, 1), Edge(3L, 6L, 1),
        //                        Edge(4L, 1L, 1), Edge(4L, 2L, 1), Edge(4L, 3L, 1), Edge(4L, 6L, 1),
        //                        Edge(5L, 1L, 1), Edge(5L, 2L, 1),
        //                        Edge(6L, 3L, 1), Edge(6L, 4L, 1), Edge(6L, 7L, 1),
        //                        Edge(7L, 6L, 1), Edge(7L, 8L, 1),
        //                        Edge(8L, 7L, 1)
        //                      ))
        //
        // val graph = Graph(vertices, edges, -1L).mapVertices((id, _) => (0, 0, Array.fill[Long](numLevels)(id))).cache()
        //


        // numLevels = graph.degrees.reduce(max)._2
        // val graph = originalGraph.mapVertices((id, _) => (0, 0, Array.fill[Long](numLevels)(id)))

        var startTime = System.currentTimeMillis()

        var degreeGraph = graph.outerJoinVertices(graph.outDegrees)((id, oldAttr, outDegOpt) => (outDegOpt.getOrElse(0), outDegOpt.getOrElse(0), oldAttr._3))
        // var degreeGraphVertices = degreeGraph.vertices.cache()
        // val degreeZeroVertices = graph.outerJoinVertices(graph.degrees)((id, oldAttr, outDegOpt) => (outDegOpt.getOrElse(0), outDegOpt.getOrElse(0), oldAttr._3)).vertices.filter{
        //   case (id, x) =>  x._1 == 0
        // }

        val pregelGraph = degreeGraph.pregel((0, 0, Array.fill[Long](numLevels)(-1L)), numLevels -1)(
          (id, attr, newAttr) => {
            if(newAttr._3(0) == -1L){
              attr
            }else if(attr._3(0) == -1L){
              newAttr
            }else if(newAttr._1 >= attr._2){
              (attr._1, newAttr._1, merge(newAttr._3, attr._3))
            }else{
              attr
            }
          }, // Vertex Program
          triplet => {  // Send Message
            if(triplet.dstAttr._1 < triplet.srcAttr._1){
              Iterator((triplet.dstId, triplet.srcAttr))
            }else{
              Iterator.empty
            }
          },
          (a,b) => {
            if(a._1 > b._2){
              (b._1, a._1, merge(a._3, b._3))
            }else{
              b
            }
          }
        )

        // pregelGraph.vertices.mapValues(x => "(" + x._1 + ", " + x._2 + ", (" + x._3.mkString(",") + "))").saveAsTextFile("/test")



        // for (x <- (numLevels - 1) to 1 by -1) {
        //   degreeGraphVertices = degreeGraph.aggregateMessages[(Int, Int, Array[Long])](
        //     triplet => {  // Send Message
        //       if(triplet != null && triplet.dstAttr != null && triplet.srcAttr != null){
        //         triplet.sendToSrc(triplet.srcAttr)
        //         triplet.sendToDst(triplet.dstAttr)
        //         if(triplet.dstAttr._1 <= x && triplet.dstAttr._1 < triplet.srcAttr._1){
        //           triplet.sendToDst(triplet.srcAttr)
        //         }
        //       }
        //     },
        //     (a,b) => {
        //       if(a._1 >= b._2){
        //         (b._1, a._1, merge(a._3, b._3))
        //       }else{
        //         b
        //       }
        //     }
        //   )
        //   // degreeGraphVertices.take(1)
        //   if(x > 1){
        //     degreeGraph = Graph(degreeGraphVertices, graph.edges)
        //   }
        // }

        val finalGraph = Graph(pregelGraph.vertices.mapValues(x => x._3), graph.edges).cache()
        var endTime = System.currentTimeMillis()
        // sc.parallelize(Array((endTime-startTime))).saveAsTextFile("/socialNet/time")

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
