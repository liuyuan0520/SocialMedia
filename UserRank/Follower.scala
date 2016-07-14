import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Follower {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("follower")
    val sc = new SparkContext(conf)
    // 一个 distinct performance 差辣么多？
    val file = sc.textFile("hdfs:///TwitterGraph.txt").distinct()
    val followees = file.map(line => line.split("\t")(1)).map(user =>(user, 1)).reduceByKey(_+_)
    val f = followees.map(f => f._1 + "\t" + f._2)
    f.saveAsTextFile("/follower-output")
  }
}

