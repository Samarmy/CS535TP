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
import java.util.Random
        
object NaiveReducer {
    val rand = new Random();
    var numNodes = 10000
    var levelSize = 100
    val numLevels = 10
    
    case class JsNode(id: Long, realID: Long)
    case class JsLink(source: Int, target: Int)
    case class JsGraph(nodes: Array[JsNode], links: Array[JsLink])
        
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
        ).cache()
        //clusteredGraph.vertices.collect.foreach(x => println(x._1 +" "+x._2.mkString(",")+"\n"))
        
        
        var gh = GraphHolder(clusteredGraph)
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


