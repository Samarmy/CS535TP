import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.commons.io.FileUtils
import com.google.gson.Gson
import java.io.{InputStream, OutputStream, File}
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.util.concurrent.Executors;
import java.net.InetSocketAddress

case class JsonHolder(j: String){
    var json = j
}

object VisualizationTest{
    case class JsNode(name: String, hashtags: String)
    case class JsLink(source: Int, target: Int)
    case class JsGraph(nodes: Array[JsNode], links: Array[JsLink])
    var jh = JsonHolder("")
    
    def main(args: Array[String]) {
      val spark = SparkSession.builder.appName("VisualizationTest").getOrCreate()
      import spark.implicits._
      val sc = spark.sparkContext
      
      val users: RDD[(VertexId, (String, String))] =
        sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
      // Create an RDD for edges
      val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
      // Define a default user in case there are relationship with missing user
      val defaultUser = ("John Doe", "Missing")
      // Build the initial Graph
      val graph = Graph(users, relationships, defaultUser)
      val verts = graph.vertices.map{
                    case (id, (name, pos)) => JsNode(name, pos)
                  }.distinct().collect()
      
      val edges = graph.edges.map{
                    edge => JsLink(edge.srcId.toInt, edge.dstId.toInt)
                  }.collect()
                  
      var gson = new Gson()
      jh.json = gson.toJson(JsGraph(verts, edges))    
      
      val server = HttpServer.create(new InetSocketAddress(11777), 0)
      server.createContext("/", new RootHandler(jh))
      server.setExecutor(null)
      server.start()
    }
}

class RootHandler(jh: JsonHolder) extends HttpHandler {
    
  def handle(t: HttpExchange) {
    displayPayload(t.getRequestBody)
    sendResponse(t)
  }

  private def displayPayload(body: InputStream): Unit ={
    println()
    println("******************** REQUEST START ********************")
    println()
    copyStream(body, System.out)
    println()
    println("********************* REQUEST END *********************")
    println()
  }

  private def copyStream(in: InputStream, out: OutputStream) {
    Iterator
      .continually(in.read)
      .takeWhile(-1 !=)
      .foreach(out.write)
  }

  private def sendResponse(t: HttpExchange) {
    //FileUtils.writeStringToFile(new File("/s/chopin/a/grad/kevincb/test.json"), json)
    t.getResponseHeaders().add("Content-Type", "application/json");
    //t.getResponseHeaders().add("Content-Encoding", "gzip");
    t.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    t.getResponseHeaders().add("Access-Control-Allow-Methods", "POST");
    t.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type,Content-Encoding");
    t.sendResponseHeaders(200, jh.json.length())
    val os = t.getResponseBody
    os.write(jh.json.getBytes)
    os.close()
  }

}
