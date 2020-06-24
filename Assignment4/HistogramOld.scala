import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {

  type Color = (Short, Short)

  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Join")
    val sc = new SparkContext(conf)

    //    //reading Histogram
    val input = sc.textFile(args(0)).cache()
    val points = input.map(line => {
      val a = line.split(",")
      (new Color(1.toShort, a(0).toShort), 1.toLong) //red
    }) ++ input.map(line => {
      val a = line.split(",")
      (new Color(2.toShort, a(1).toShort), 1.toLong) //green
    }) ++ input.map(line => {
      val a = line.split(",")
      (new Color(3.toShort, a(2).toShort), 1.toLong) //green
    })

    val countRDD = points.map(p => (p._1, p._2)).groupByKey().map(p => {
      var count = 0l
      for (c <- p._2) {
        count = count + c.toLong
      }
      (p._1, count)
    })
    countRDD.collect.foreach(h => println(h._1._1 + "\t" + h._1._2 + "\t" + h._2))
  }
}
