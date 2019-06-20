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

//class VertexProperty()
case class clusterProperty(l: Array[Long], ix: Int) extends VertexProperty
    
case class GraphHolder(g: Graph[clusterProperty,Int]){
    var graph = g
}

case class JsNode(id: Long, realID: Long)
case class JsLink(source: Int, target: Int)
case class JsGraph(nodes: Array[JsNode], links: Array[JsLink])
    
object NaiveReducer {
    val rand = new Random();
    
    def merge(a: clusterProperty, b1: clusterProperty) : clusterProperty = {
        val b = clusterProperty(b1.l.toList.filter(!a.l.contains(_)).toArray, b1.ix)
        val answer = clusterProperty(Array.fill(a.l.length+b.l.length){0L}, -1)
        var i = 0
        var j = 0
        var k = 0;
        while (i < a.l.length && j < b.l.length) {
            //if (a.l(i) < b.l(j)){
            if (rand.nextFloat() < 0.5){
                answer.l(k) = a.l(i);
                i += 1;
                k += 1;
            }else{
                answer.l(k) = b.l(j);
                j += 1;
                k += 1;
            }
        }
        while (i < a.l.length){
            answer.l(k) = a.l(i);
            i += 1;
            k += 1;
        }
        while (j < b.l.length) {
            answer.l(k) = b.l(j);
            j += 1;
            k += 1;
        }
        return answer
    }
    
    def append(a: clusterProperty, b1: clusterProperty) : clusterProperty = {
        val b = clusterProperty(b1.l.toList.filter(!a.l.contains(_)).toArray, b1.ix)
        var answer = clusterProperty(a.l ++ b.l, a.l.length-1)
        if(b.l.length == 0 && b1.ix != -2){
            answer = clusterProperty(answer.l, answer.l.length)
        }
        return answer
    }
    

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("NaiveReducer").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext
        
        val graph: Graph[Long, Int] = graphx.util.GraphGenerators.logNormalGraph(sc, 10000, 0, 0.4, 0.5)
        
        val initialGraph = graph.mapVertices((id, prop) => {
            clusterProperty(Array(id),0)
        })
          
        val clusteredGraph = initialGraph.pregel(clusterProperty(Array[Long](),-2), 6)(
        (id, cp, newCp) => append(cp, newCp), // Vertex Program
        triplet => {  // Send Message
            if (triplet.srcAttr.ix != triplet.srcAttr.l.length){
                Iterator((triplet.dstId, clusterProperty(triplet.srcAttr.l.slice(triplet.srcAttr.ix, triplet.srcAttr.l.length),-1)))
            } else {
                Iterator.empty
        }
        },
        (a, b) => merge(a, b) // Merge Message
        ).cache()
        //clusteredGraph.vertices.collect.foreach(x => println(x._1 +" "+x._2.ix+" "+x._2.l.mkString(",")+"\n"))
        
        var gh = GraphHolder(clusteredGraph)
        val server = HttpServer.create(new InetSocketAddress(11777), 0)
        server.createContext("/", new ResponseHandler(gh))
        server.setExecutor(null)
        server.start()
        
        //spark.stop()
    }
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
    val request = body.split(",").map(_.toInt)
    val vizVerts = gh.graph.vertices.flatMap{case (id, p: clusterProperty) =>
        if (request(1) == -1){
            Array[JsNode](JsNode(p.l.find(_ < request(0)).getOrElse(-1), id))
        } else {
            val startingIx : Long = p.l.find(_ < request(0).toInt).getOrElse(-1)
            if (startingIx == 0){
                 Array[JsNode](JsNode(request(1), id))
            } else if (startingIx != -1 && startingIx == request(1)){
                //Array[JsNode](JsNode(p.l.slice(startingIx.toInt,p.l.length).find(_ < request(0).toInt).getOrElse(-1), id))
                Array[JsNode](JsNode(id, id))
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
    val response = makeResponse(body)
    t.sendResponseHeaders(200, response.length())
    val os = t.getResponseBody
    os.write(response.getBytes)
    os.close()
  }
}
