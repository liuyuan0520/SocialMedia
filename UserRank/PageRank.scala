import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PageRank {
    def main(args: Array[String]) {
	    val conf = new SparkConf().setAppName("PageRank")
	    val sc = new SparkContext(conf)
	    val file = sc.textFile("hdfs:///TwitterGraph.txt")

	    // Generate a table whose format is (follower, (followee1, followee2, followee3, ... , folloween)
	    val whoDoFollowRDD = file.map(line => (line.split("\t")(0), line.split("\t")(1))).distinct().groupByKey().cache()
	    
	    // Gernerate the list of followers, the list of followees and the list of all users
		val followers = whoDoFollowRDD.keys.cache()
		val followees = whoDoFollowRDD.values.flatMap(followee => followee).distinct().cache()
		val users = (followees ++ followers).distinct().cache()
		val usersCount = users.count()
		
		// Generate the list of those who do not follow others and those who are not followed by others
		val whoDoNotFollow = users.subtract(followers).cache()
		val whoAreNotFollowed = users.subtract(followees).cache()

		// Generate a complete table
		val whoDoNotFollowRDD = whoDoNotFollow.map(user => (user, Iterable("DummyField"))).cache()
		val fullRDD = (whoDoFollowRDD ++ whoDoNotFollowRDD).cache()

		// Generate a table for ranks
		var ranksRDD = fullRDD.map(line => (line._1, 1.00)).cache()
		// Take those IDs that can never receive any contributions into consideration
		val whoAreNotFollowedRDD = whoAreNotFollowed.map(user => (user, 0.00)).cache()

		// Iterate 10 times
		for (i <- 0 to 9) {
			val pool = sc.accumulator(0.00)

			// For those have followees, calculate their contributions
			val whoDoFollowJoinRDD = whoDoFollowRDD.join(ranksRDD).values.flatMap {
				case(followees, rank) => {
					followees.map(followee => (followee, rank.toDouble / followees.size))
				}
			}

			// For those who don't have followees, calculate the amount that will be redistributed
			val whoDoNotFollowJoinRDD = whoDoNotFollowRDD.join(ranksRDD)
			whoDoNotFollowJoinRDD foreach {
				case (follower, (dummy, rank)) => (pool += rank.toDouble)
			}

			// Take those IDs that can never receive any contributions into consideration
			val resultRDD = whoDoFollowJoinRDD ++ whoAreNotFollowedRDD

			// Update the ranks
			val sum = pool.value
			ranksRDD = resultRDD.reduceByKey(_ + _).mapValues(rank => 0.15 + 0.85 * (sum / usersCount + rank.toDouble))
		}

		// Output 
		val output = ranksRDD.map(f => f._1 + "\t" + f._2)
    	output.saveAsTextFile("/pagerank-output")
    }
}