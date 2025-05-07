import org.apache.spark.sql.SparkSession

object LogFileAnalytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Log File Analytics")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val logFile = "access_log.txt"  // Change to your log file path
    val logs = sc.textFile(logFile)

    // (I) Count of 404 response codes
    val notFound404 = logs.filter(line => line.contains(" 404 "))
    val count404 = notFound404.count()
    println(s"\n(I) Total number of 404 errors: $count404")

    // (II) Top 25 Hosts that caused 404
    val top25Hosts = notFound404
      .map(line => line.split(" ")(0))  // Extract host/IP
      .map(host => (host, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(25)

    println("\n(II) Top 25 Hosts with 404 errors:")
    top25Hosts.foreach { case (host, count) =>
      println(s"$host: $count")
    }

    // (III) Unique Daily Hosts
    val dailyHosts = logs
      .map { line =>
        val parts = line.split(" ")
        val host = parts(0)
        val datePart = parts(3).substring(1, 12) // Extract date from [01/Jul/1995:...
        ((datePart, host), 1)
      }
      .reduceByKey((_, _) => 1)
      .map { case ((date, _), _) => (date, 1) }
      .reduceByKey(_ + _)

    println("\n(III) Unique Daily Hosts:")
    dailyHosts.collect().foreach { case (date, count) =>
      println(s"$date: $count hosts")
    }

    spark.stop()
  }
}
