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

object SEQ {
    var numLevels = 4
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

    def getOne(a: Array[(Long, (Set[Long], Set[Long]), Array[Long])]) : Array[(Long, (Set[Long], Set[Long]), Array[Long])] = {
        var trackSets = Array.empty[(Set[Long], Set[Long])]
        var ary = Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])]
        for(x <- a.sortWith(_._1 < _._1)){
          if(!trackSets.contains(x._2)){
            trackSets = trackSets :+ x._2
            ary = ary :+ x
          }
        }
        return ary
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("SEQ").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        val vertices: RDD[(VertexId, Long)] =
          sc.parallelize(Array((1L, 0L),
                               (2L, 0L),
                               (3L, 0L),
                               (4L, 0L),
                               (5L, 0L),
                               (6L, 0L),
                               (7L, 0L),
                               (8L, 0L),
                               (9L, 0L),
                               (10L, 0L),
                               (11L, 0L),
                               (12L, 0L),
                               (13L, 0L),
                               (14L, 0L),
                               (15L, 0L),
                               (16L, 0L),
                               (17L, 0L),
                               (18L, 0L)
                             ))

        val edges: RDD[Edge[Int]] =
          sc.parallelize(Array(Edge(2L, 1L, 1),
                               Edge(3L, 1L, 1),
                               Edge(4L, 1L, 1),
                               Edge(5L, 2L, 1),Edge(5L, 3L, 1), Edge(5L, 4L, 1), Edge(5L, 10L, 1),Edge(5L, 11L, 1), Edge(5L, 12L, 1), Edge(5L, 14L, 1),Edge(5L, 15L, 1), Edge(5L, 16L, 1),
                               Edge(6L, 5L, 1), Edge(6L, 9L, 1),
                               Edge(7L, 5L, 1), Edge(7L, 9L, 1),
                               Edge(8L, 5L, 1), Edge(8L, 9L, 1),
                               Edge(13L, 10L, 1),
                               Edge(13L, 11L, 1),
                               Edge(13L, 12L, 1),
                               Edge(14L, 5L, 1), Edge(14L, 17L, 1),
                               Edge(15L, 5L, 1), Edge(15L, 17L, 1),
                               Edge(16L, 5L, 1), Edge(16L, 17L, 1),
                               Edge(17L, 14L, 1),Edge(17L, 15L, 1), Edge(17L, 16L, 1)
                             ))

        val graph = Graph(vertices, edges, -1L).cache()

        // var graph = graphx.util.GraphGenerators.logNormalGraph(sc, numNodes, 0, 0.1, 1.0)
        // graph = Graph(graph.vertices, graph.edges ++ graph.reverse.edges)

        val initializedGraph = graph.mapVertices((id, attr) => {
          (Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])], (Set.empty[Long], Set.empty[Long]), Array.fill[Long](numLevels)(id))
        }).cache()

        var setGraph = initializedGraph.joinVertices(initializedGraph.collectNeighborIds(EdgeDirection.Out))((id, oldAttr, outNeighbors) => (oldAttr._1, (outNeighbors.toSet, oldAttr._2._2), oldAttr._3)).joinVertices(initializedGraph.collectNeighborIds(EdgeDirection.In))((id, oldAttr, inNeighbors) => (oldAttr._1, (oldAttr._2._1, inNeighbors.toSet), oldAttr._3)).cache()

        var degreeZeroVertices = setGraph.vertices.filter{
          case (id, x) =>  x._2._1.isEmpty && x._2._2.isEmpty
        }.cache()
        
        var setGraphVertices = setGraph.aggregateMessages[(Array[(Long, (Set[Long], Set[Long]), Array[Long])], (Set[Long], Set[Long]), Array[Long])](
          triplet => {  // Send Message
            if(triplet != null && triplet.dstAttr != null && triplet.srcAttr != null){
              triplet.sendToDst(triplet.dstAttr)
              triplet.sendToSrc(triplet.srcAttr)
              triplet.sendToDst((Array((triplet.srcId.toLong, triplet.srcAttr._2, triplet.srcAttr._3)), (Set.empty[Long], Set.empty[Long]), Array.empty[Long]))
              triplet.sendToSrc((Array((triplet.dstId.toLong, triplet.dstAttr._2, triplet.dstAttr._3)), (Set.empty[Long], Set.empty[Long]), Array.empty[Long]))
            }
          },
          (a,b) => {
            (a._1 ++ b._1, (a._2._1 , a._2._2 ), a._3)
          }
        ).mapValues(x => {
            if(x._1.size > 1){
              (getOne(x._1), x._2, x._3)
            }else{
              x
            }
          }
        ).cache()

        var indexedGraph = Graph(setGraphVertices, graph.edges).cache()

        var indexedGraphVertices = indexedGraph.aggregateMessages[(Array[(Long, (Set[Long], Set[Long]), Array[Long])], (Set[Long], Set[Long]), Array[Long])](
          triplet => {  // Send Message
            if(triplet != null && triplet.dstAttr != null && triplet.srcAttr != null){
              triplet.sendToDst(triplet.dstAttr)
              triplet.sendToSrc(triplet.srcAttr)
              var dstAry = Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])]
              var srcAry = Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])]
              for(x <- triplet.srcAttr._1){
                if(triplet.dstId.toLong != x._1 && triplet.dstAttr._2 == x._2){
                  dstAry = dstAry :+ x
                }
              }
              for(x <- triplet.dstAttr._1){
                if(triplet.srcId.toLong != x._1 && triplet.srcAttr._2 == x._2){
                  srcAry = srcAry :+ x
                }
              }

              if(!dstAry.isEmpty){
                triplet.sendToDst((Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])], (Set.empty[Long], Set.empty[Long]), dstAry(0)._3))
              }else if(!srcAry.isEmpty){
                triplet.sendToSrc((Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])], (Set.empty[Long], Set.empty[Long]), srcAry(0)._3))
              }
            }
          },
          (a,b) => {
            (a._1, (a._2._1, a._2._2), merge(b._3, a._3))
          }
        ).cache()

        val finalGraph = Graph(indexedGraphVertices.mapValues(x => x._3) ++ degreeZeroVertices.mapValues(x => x._3), graph.edges).cache()

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
