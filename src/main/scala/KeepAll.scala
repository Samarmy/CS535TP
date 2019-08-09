import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexRDD

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.io.{InputStream, OutputStream, File}
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress
import scala.io.Source
import scala.collection.{mutable, Map}
import scala.reflect.ClassTag

object ShortestPaths extends Serializable {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Array[Long]]

  private def makeMap(x: (VertexId, Array[Long])*) = Map(x: _*)

  private def incrementMap(spmap: SPMap, vertex: Long): SPMap = spmap.map { case (v, d) => v -> (vertex +: d) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
    // Mimics the optimization of breakOut, not present in Scala 2.13, while working in 2.12
    val map = mutable.Map[VertexId, Array[Long]]()
    (spmap1.keySet ++ spmap2.keySet).foreach { k => {
        var val1 = spmap1.getOrElse(k, Int.MaxValue)
        if(val1 != Int.MaxValue){
          val1 = val1.asInstanceOf[Array[Long]].size
        }
        var val2 = spmap2.getOrElse(k, Int.MaxValue)
        if(val2 != Int.MaxValue){
          val2 = val2.asInstanceOf[Array[Long]].size
        }
        if(val1.asInstanceOf[Int] < val2.asInstanceOf[Int]){
          map.put(k, spmap1.getOrElse(k, Array.empty[Long]))
        }else{
          map.put(k, spmap2.getOrElse(k, Array.empty[Long]))
        }
      }
    }
    map
  }

  /**
   * Computes shortest paths to the given set of landmark vertices.
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[SPMap, ED] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> Array.empty[Long]) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr, edge.dstId)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }
}


object KeepOne {
    var numLevels = 5
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

    def update(a: (Int, Boolean, Array[Long]), b: (Int, Boolean, Array[Long])) : (Int, Boolean, Array[Long]) = {
        if(a._3(0) == -1L){
          return b
        }else if(b._3(0) == -1L){
          return a
        }else{
          var n = getLevel(b._3)
          var retArray = a._3.clone
          if(n <= retArray.size){
            for ( x <- 0 to n - 1) retArray(x) = b._3(x)
          }
          return (a._1, b._2, retArray)
        }
    }

    def minSpanningTree[VD:scala.reflect.ClassTag](g:Graph[(Int, Boolean, Array[Long]),(Int, Array[Long])]) = {
      var g2 = g.mapEdges(e => (e.attr,false))

      for (i <- 1L to g.vertices.count-1) {
        val unavailableEdges = g2.outerJoinVertices(g2.subgraph(_.attr._2).connectedComponents.vertices)((vid,vd,cid) => (vd,cid)).subgraph(et => et.srcAttr._2.getOrElse(-1) == et.dstAttr._2.getOrElse(-2)).edges.map(e => ((e.srcId,e.dstId),e.attr))

        type edgeType = Tuple2[Tuple2[VertexId,VertexId],(Int, Array[Long])]
        val smallestEdge = g2.edges.map(e => ((e.srcId,e.dstId),e.attr)).leftOuterJoin(unavailableEdges).filter(x => !x._2._1._2 && x._2._2.isEmpty).map(x => (x._1, x._2._1._1)).min()(new Ordering[edgeType]() {
          override def compare(a:edgeType, b:edgeType) = {
            val r = Ordering[Int].compare(a._2._1,b._2._1)
            if (r == 0)
              Ordering[Long].compare(a._1._1, b._1._1)
            else
              r
          }
        })
        g2 = g2.mapTriplets(et =>(et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1 && et.dstId == smallestEdge._1._2)))
      }
      g2.subgraph(_.attr._2).mapEdges(_.attr._1)
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("KeepOne").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext
        //sc.setCheckpointDir("hdfs://salt-lake-city:30121/pregel_checkpoint")
        
        //var edges = sc.textFile("hdfs://austin:30121/socialNet2/*").map(x => {
          //var ary = x.split("\\s+")
            //Edge(ary(0).toLong, ary(1).toLong, 1)
        //})
        
        
        val edges = sc.textFile("hdfs://austin:30121/roadNet/roadNet-CA.txt").filter(!_.contains("#")).map(x => {
          var ary = x.split("\\s+")
            Edge(ary(0).toLong, ary(1).toLong, 1)
        })
        
        var graph = Graph.fromEdges(edges, 0L).mapVertices((id, _) => (0, 0, Array.fill[Long](numLevels)(id)))
        println("Initial graph constructed")
        // val vertices: RDD[(VertexId, Long)] =
        //   sc.parallelize(Array((1L, 0L),
        //                        (2L, 0L),
        //                        (3L, 0L),
        //                        (4L, 0L),
        //                        (5L, 0L),
        //                        (6L, 0L),
        //                        (7L, 0L)
        //                      ))
        //
        // val edges: RDD[Edge[Int]] =
        //   sc.parallelize(Array(Edge(1L, 2L, 1), Edge(1L, 3L, 1), Edge(1L, 4L, 1),
        //                        Edge(2L, 1L, 1),
        //                        Edge(3L, 1L, 1),
        //                        Edge(4L, 1L, 1), Edge(4L, 5L, 1),
        //                        Edge(5L, 4L, 1), Edge(5L, 6L, 1), Edge(5L, 7L, 1),
        //                        Edge(6L, 5L, 1),
        //                        Edge(7L, 5L, 1)
        //                      ))
        //
        // val graph = Graph(vertices, edges).mapVertices((id, _) => (0, false, Array.fill[Long](numLevels)(id))).cache()
        //
        // var graph = graphx.util.GraphGenerators.logNormalGraph(sc, numNodes, 0, 4.0, 1.0).mapVertices((id, _) => (0, false, Array.fill[Long](numLevels)(id))).cache()

        //Find degree for each vertice
        var degreeGraph = graph.outerJoinVertices(graph.degrees)((id, oldAttr, DegOpt) => (DegOpt.getOrElse(0), false, oldAttr._3))
        var degreeGraphVertices = degreeGraph.vertices
        //degreeGraphVertices.collect.foreach(println)

        //Keep vertices with 0 degree (Pregel only recognizes triplets so these are left out at the end)
        val degreeZeroVertices = degreeGraph.vertices.filter{
          case (id, x) =>  x._1 == 0
        }

        val estimatedAvgPathLength = ShortestPaths.run(degreeGraph, degreeGraph.vertices.sample(false, 10.0/degreeGraph.vertices.count).map(x => x._1).collect())
            .vertices.flatMap(x => {
                var summation = 0
                var count = 0
                x._2.foreach(y => {
                    summation += y._2.size
                    count += 1
                })
                if (count != 0){
                    Iterator((summation/count))
                } else {
                    Iterator.empty
                }
            }).mean

        println("Estimated average path length: "+ estimatedAvgPathLength)
        val importantDegree = degreeGraph.vertices.map{
            case (id, x) => x._1
        }.top((100.0/estimatedAvgPathLength).toInt).reduceLeft(_ min _)
        
        //Pick important vertices. 
        val iV = degreeGraph.vertices.filter{
          case (id, x) =>  x._1 >= importantDegree
        }

        val importantVertices = iV.sample(false, Math.min((100.0/estimatedAvgPathLength)/iV.count,1))
        println("Selected "+importantVertices.count+" important vertices")

        //println(savedIV.value)
        println("Beginning shortest path calculation")
        var startTime = System.currentTimeMillis()
        //Get Shortest Paths of all important vertices
        //importantEdges1.vertices.collect.foreach{case (key, values) => println("key " + key + " - " + values.map{case (k,v) => ""+k+": "+v.mkString("-")})}
        val importantEdges = ShortestPaths.run(degreeGraph, importantVertices.map(x => x._1).collect()).vertices.flatMap(x => {
          var ary = Array.empty[(Long, Edge[(Int, Array[Long])])]
          x._2.foreach(y => {
            ary = ary :+ (x._1, Edge(x._1, y._1, (y._2.size, y._2)))
          })
          ary
        }).filter(x => x._2.srcId != x._2.dstId).join(importantVertices).map(x => x._2._1)
        println("Finished shortest path calculation in "+(System.currentTimeMillis()-startTime)+" milliseconds")
        //importantEdges.collect.foreach(println)
        val completeGraph = Graph(importantVertices, importantEdges)
        
        //completeGraph.vertices.collect.foreach(x => println(x._2._3.mkString(" ")))

        //Find minimum spanning tree of important vertices graph
        //startTime = System.currentTimeMillis()
        //var k1 = minSpanningTree(completeGraph)
        //k1.vertices.collect.foreach(println)
        //println("Minimumum spanning tree calculated in "+(System.currentTimeMillis()-startTime)+" milliseconds")
        //Get original paths for important vertices (including vertices and edges not in important vertices but on paths between important vertices)
        
        val k2Edges = completeGraph.edges.flatMap(x => {
          var ary = Array(Edge(x.srcId, x.attr._2(0), 1))
          for(y <- 0 to (x.attr._2.size - 2)){
            ary = ary :+ Edge(x.attr._2(y), x.attr._2(y+1), 1)
          }
          ary
        })
        //k2Edges.collect.foreach(println)
        //completeGraph.vertices.collect.foreach(println)
        
        //Union important vertices and vertices on important vertices paths (second variable set to true) with non-important vertices (second variable set to false)
        //println("Complete graph vertices: ")
        //Graph(completeGraph.vertices, k2Edges, (0, true, Array.fill[Long](numLevels)(-1L))).vertices.collect.foreach(println)
        val totalVertices = Graph(completeGraph.vertices, k2Edges, (0, true, Array.fill[Long](numLevels)(-1L))).vertices.map(x => {
          if(x._2._3(0) == -1L){
            (x._1, (0, true, Array.fill[Long](numLevels)(x._1)))
          }else{
            (x._1, (x._2._1, true, x._2._3))
          }
        }).union(degreeGraphVertices).reduceByKey((x,y) => {
          if(x._2 == true){
            x
          }else{
            y
          }
        })

        val totalGraph = Graph(totalVertices, degreeGraph.edges)
        //totalGraph.vertices.collect().foreach(println)
        
        
        //ompleteGraph.vertices.collect.foreach(x => println(x._2._3.mkString(",")))
        
        //merge non-important arrays with important arrays
        startTime = System.currentTimeMillis()
        val pregelGraph = totalGraph.pregel((0, false, Array.fill[Long](numLevels)(-1L)))(
          (id, attr, newAttr) => update(attr, newAttr), // Vertex Program
          triplet => {  // Send Message
            if(triplet.srcAttr._3(0) == -1L){
              Iterator((triplet.srcId, triplet.srcAttr))
            }else if (!triplet.srcAttr._2 && triplet.dstAttr._2) {
              Iterator((triplet.srcId, triplet.dstAttr))
            } else {
              Iterator.empty
            }
          },
          (a, b) => update(a,b) // Merge Message
        ).subgraph(vpred = (id, attr) => attr._3.exists(_ != attr._3.head))
        
        
        //val finalGraph = Graph(pregelGraph.vertices.mapValues(x => x._3) ++ degreeZeroVertices.mapValues(x => x._3), pregelGraph.edges).cache()
        val finalGraph = Graph(pregelGraph.vertices.mapValues(x => x._3), pregelGraph.edges).cache()
        println("Final graph complete in "+(System.currentTimeMillis()-startTime)+" milliseconds")
        
        //finalGraph.vertices.collect().foreach(x => println(x._2.mkString(" ")))
        
        var gh = GraphHolder(finalGraph)
        val server = HttpServer.create(new InetSocketAddress(11777), 0)
        server.createContext("/", new ResponseHandler(gh))
        server.setExecutor(null)
        println("Server started")
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
