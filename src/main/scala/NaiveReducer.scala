import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

import com.google.gson.Gson
import java.io.{InputStream, OutputStream, File}
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress
import scala.io.Source
import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import java.util.Random
        
object NaiveReducer {
    val rand = new Random();
    var numNodes = 10000000
    var levelSize = 100
    val numLevels = 10
    
    case class JsNode(id: Long, realID: Long)
    case class JsLink(source: Int, target: Int)
    case class JsAdjNode(id: Long)
    case class JsGraph(nodes: Array[JsNode], links: Array[JsLink], adjacentSubgraphs: Array[JsAdjNode], algoTime: Long, queryTime: Long)
        
    def merge(a: Array[Long], b: Array[Long]) : Array[Long] = {
        var retArray = Array.fill[Long](a.length)(-1)
        for (i <- a.indices){
            if (a(i) != -1 && b(i) != -1){
                if (rand.nextFloat() < 0.5){
                    retArray(i) = a(i)
                } else {
                    retArray(i) = b(i)
                }
            } else if (a(i) != -1 && b(i) == -1){
                retArray(i) = a(i)
            } else if (a(i) == -1 && b(i) != -1){
                retArray(i) = b(i)
            }
        }
        return retArray
    }
        
    def update(a: Array[Long], b: Array[Long]) : Array[Long] = {
        var retArray = Array.fill[Long](a.length)(-1)
        for (i <- retArray.indices){
            if (a(i) != -1){
                retArray(i) = a(i)
            } else {
                retArray(i) = b(i)
            }
        }
        return retArray
    }
        
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("NaiveReducer").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext
        sc.setCheckpointDir("hdfs://austin:30121/kevin/")
        //clusteredGraph.vertices.collect.foreach(x => println(x._1 +" "+x._2.mkString(",")+"\n"))
        
        val (clusteredGraph, time) = makeGraph(sc, -1)
        
        var gh = GraphHolder(clusteredGraph)
        println("server started")
        val server = HttpServer.create(new InetSocketAddress(11777), 0)
        server.createContext("/", new ResponseHandler(gh, time, sc))
        server.setExecutor(null)
        server.start()
        
        //spark.stop()
    }
    
    def makeGraph(sc: SparkContext, subgraphSize: Double): Tuple2[Graph[Array[Long],Int], Long] = {
        val graph: Graph[Long, Int] = graphx.util.GraphGenerators.logNormalGraph(sc, numNodes, 0, 0.4, 1.0)
        
        val initialGraph = graph.mapVertices((id, _) => {
            var retArray = Array.fill[Long](numLevels)(-1)
            var a = 0
            var ix = 0
            while( a < numLevels*levelSize){
                a += levelSize
                if (id < a){
                    retArray(ix) = id
                    a = numLevels*levelSize+1
                }
                ix += 1
            }
            (retArray)
        })
        //initialGraph.vertices.collect.foreach(x => println(x._1 +" "+x._2.mkString(",")+"\n"))

        var startTime = System.nanoTime()
        val clusteredGraph = initialGraph.pregel(Array.fill[Long](numLevels)(-1))(
            (id, cp, newCp) => update(cp, newCp), // Vertex Program
            triplet => {  // Send Message
                var msg = false
                for (i <- triplet.srcAttr.indices){
                    if (triplet.dstAttr(i) == -1 && triplet.srcAttr(i) != -1){
                        msg = true
                    }
                }
                if (msg){
                    Iterator((triplet.dstId, triplet.srcAttr))
                } else {
                    Iterator.empty
                }
            },
            (a, b) => merge(a, b) // Merge Message
        ).subgraph(vpred = (id, attr) => attr.exists(_ != attr.head)).cache()
        
        (clusteredGraph, System.nanoTime()-startTime)
    }
    
    class ResponseHandler(gh: GraphHolder, at: Long, sc: SparkContext) extends HttpHandler {
        
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
            var g = gh.graph
            var algoTime = at
            var bodyText = body
            var cache = true
            if (body.startsWith("benchmark")){
                gh.graph.unpersist()
                val benchmarkInfo = body.split(":")
                if (benchmarkInfo.length > 3){
                    bodyText = benchmarkInfo(3)
                } else {
                    bodyText = "";
                }
                if (!benchmarkInfo(1).equals("nocache")){
                    cache = false
                }
                benchmarkInfo.foreach(println)
                println("---------------------------------------------------------------------")
                var output = makeGraph(sc, benchmarkInfo(2).toDouble)
                g = output._1
                algoTime = output._2
            }
        
            var gson = new Gson()
            val request = body.split(",").filter(_!="").map(_.toInt)
            val l = request.length
        
            val adjacentSubgraphs = Future {
                if (l == 0){
                    Array[JsAdjNode]()
                } else {
                    g.triplets.flatMap{ t =>
                        if(t.srcAttr.take(l).sameElements(request) && t.dstAttr.take(l-1).sameElements(request.dropRight(1))){
                            Array[JsAdjNode](JsAdjNode(t.dstAttr(l-1)))
                        } else if(t.dstAttr.take(l).sameElements(request) && t.srcAttr.take(l-1).sameElements(request.dropRight(1))){
                            Array[JsAdjNode](JsAdjNode(t.srcAttr(l-1)))
                        }else {
                            Array[JsAdjNode]()
                        }
                    }.distinct().collect()
                }
            }
            
            val vizVerts = Future { g.vertices.flatMap{case (id, prop) =>
                    if (request.length == 0){
                        Array[JsNode](JsNode(prop(0), id))
                    } else {
                        if(prop.take(l).sameElements(request)){
                            if (l == numLevels){
                                Array[JsNode](JsNode(id, id))
                            } else {
                                Array[JsNode](JsNode(prop(l), id))
                            }
                        } else {
                            Array[JsNode]()
                        }
                    }
                }.distinct().collect()
            }

            val vizEdges = Future { g.edges.map{
                    edge => JsLink(edge.srcId.toInt, edge.dstId.toInt)
                }.collect()
            }
            
            var startTime = System.currentTimeMillis()
            val aggFut = Await.ready(for{
                f1Result <- vizVerts
                f2Result <- vizEdges
                f3Result <- adjacentSubgraphs
            } yield (f1Result, f2Result, f3Result), Duration.Inf).value.get

            aggFut match {
                case Success(v) =>
                    return gson.toJson(JsGraph(v._1, v._2, v._3, algoTime, System.currentTimeMillis()-startTime))
                case Failure(e) =>
                    return "Future failed"
            }
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


