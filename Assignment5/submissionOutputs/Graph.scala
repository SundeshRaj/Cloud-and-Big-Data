import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Graph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphProcessing")
    val sc = new SparkContext(conf)
    var ReadGraph = sc.textFile(args(0)).map(line => {
      var a = line.split(",")
      var item = new ListBuffer[Long]()
      for (i <- 1 to (a.length - 1)) { item += a(i).toLong }
      var adj = item.toList
      (a(0).toLong, a(0).toLong, adj)
    })

    var graphMap = ReadGraph.map(value => (value._1, value))
    for (i <- 1 to 5) {
      ReadGraph = ReadGraph.flatMap(value => {
        var mNode = new ListBuffer[(Long, Long)]()
        mNode += ((value._1, value._2))
        for (i <- 0 to (value._3.length - 1)) {
          mNode += ((value._3(i), value._2))
        }
        var AdjNode = mNode.toList
        (AdjNode)
      })
        .reduceByKey((t1, t2) => (if (t1 >= t2) t2 else t1)).join(graphMap).map(value => (value._2._2._2, value._2._1, value._2._2._3))
    }

    val graphVal = ReadGraph.map(value => (value._2, 1)).reduceByKey((x, y) => (x + y))
    val sorting = graphVal.sortByKey(true, 0).collect()
    val get_ouptut = sorting.foreach(println)
  }
}