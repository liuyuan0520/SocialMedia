import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Follower {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("follower")
    val sc = new SparkContext(conf)

    val file = sc.textFile("hdfs:///TwitterGraph.txt")
    val followees = file.map(line => line.split("\t")(1)).map(user =>(user, 1)).reduceByKey(_+_)
    val f = followees.map(f => f._1 + "\t" + f._2)
    f.saveAsTextFile("/follower-output")
  }
}

