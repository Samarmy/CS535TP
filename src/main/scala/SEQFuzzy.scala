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

object SEQFuzzy {
    var numLevels = 2
    var simularityThreshold = 1.0
    var numIter = 5.0
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
        var found = false
        for(x <- a.sortWith((d1, d2) => (d1._2._1.size + d1._2._2.size) > (d2._2._1.size + d2._2._2.size))){
          for(y <- trackSets){
            if(x._2._1.subsetOf(y._1) && x._2._2.subsetOf(y._2)){
              found = true
            }
          }
          if(!found){
            trackSets = trackSets :+ x._2
            ary = ary :+ x
          }
          found = false
        }
        return ary
    }

    //Must Be Subset Jaccard Version
    // def jaccard(b: (Set[Long], Set[Long]), a: (Set[Long], Set[Long])) : Double = {
    //   var value = 0.0
    //   if(a._1.isEmpty && b._1.isEmpty && a._2.isEmpty && b._2.isEmpty){
    //     value = -1.0
    //   }else if(a._1.isEmpty && b._1.isEmpty && b._2.subsetOf(a._2)){
    //     value = 1.0 + (a._2.intersect(b._2).size.toDouble/(a._2++b._2).size.toDouble)
    //   }else if(a._2.isEmpty && b._2.isEmpty && b._1.subsetOf(a._1)){
    //     value = 1.0 + (a._1.intersect(b._1).size.toDouble/(a._1++b._1).size.toDouble)
    //   }else if(b._1.subsetOf(a._1) && b._2.subsetOf(a._2)){
    //     value = (a._1.intersect(b._1).size.toDouble/(a._1++b._1).size.toDouble) + (a._2.intersect(b._2).size.toDouble/(a._2++b._2).size.toDouble)
    //   }else{
    //     value = -1.0
    //   }
    //   return value
    // }

    def jaccard(a: (Set[Long], Set[Long]), b: (Set[Long], Set[Long])) : Double = {
      var value = 0.0
      if(a._1.isEmpty && b._1.isEmpty && a._2.isEmpty && b._2.isEmpty){
        value = 2.0
      }else if(a._1.isEmpty && b._1.isEmpty){
        value = 1.0 + (a._2.intersect(b._2).size.toDouble/(a._2++b._2).size.toDouble)
      }else if(a._2.isEmpty && b._2.isEmpty){
        value = 1.0 + (a._1.intersect(b._1).size.toDouble/(a._1++b._1).size.toDouble)
      }else{
        value = (a._1.intersect(b._1).size.toDouble/(a._1++b._1).size.toDouble) + (a._2.intersect(b._2).size.toDouble/(a._2++b._2).size.toDouble)
      }
      return value
    }

    def distinctUnion(a: Array[(Long, (Set[Long], Set[Long]), Array[Long])], b: Array[(Long, (Set[Long], Set[Long]), Array[Long])]) : Array[(Long, (Set[Long], Set[Long]), Array[Long])] = {
      var trackIdx = Array.empty[Long]
      var ary = Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])]
      for(x <- (a ++ b)){
        if(!trackIdx.contains(x._1)){
          trackIdx = trackIdx :+ x._1
          ary = ary :+ x
        }
      }
      return ary
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("SEQFuzzy").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        var graph = graphx.util.GraphGenerators.logNormalGraph(sc, numNodes, 0, 4.0, 1.0)
        graph = Graph(graph.vertices, graph.edges ++ graph.reverse.edges)

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
            (distinctUnion(a._1, b._1), (a._2._1 , a._2._2 ), a._3)
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
        var indexedGraphVertices = indexedGraph.vertices

        for (step <- 2.0 to simularityThreshold by -((2.0-simularityThreshold)/numIter)) {
          indexedGraphVertices = indexedGraph.aggregateMessages[(Array[(Long, (Set[Long], Set[Long]), Array[Long])], (Set[Long], Set[Long]), Array[Long])](
            triplet => {  // Send Message
              if(triplet != null && triplet.dstAttr != null && triplet.srcAttr != null){
                triplet.sendToDst(triplet.dstAttr)
                triplet.sendToSrc(triplet.srcAttr)
                var dstAry = Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])]
                var srcAry = Array.empty[(Long, (Set[Long], Set[Long]), Array[Long])]
                for(x <- triplet.srcAttr._1){
                  if(triplet.dstId.toLong != x._1 && jaccard(triplet.dstAttr._2, x._2) >= step){
                    dstAry = dstAry :+ x
                  }
                }
                for(x <- triplet.dstAttr._1){
                  if(triplet.srcId.toLong != x._1 && jaccard(triplet.srcAttr._2, x._2) >= step){
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

          indexedGraph = Graph(indexedGraphVertices, graph.edges).cache()
        }

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
