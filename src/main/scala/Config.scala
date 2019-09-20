import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds}

object Config {

  val zkHost = "localhost:2181"
  val brokerHostList = "localhost:9092"
  val kafkaTopic = "events"

  val checkpointDir = "file://~/data/spark/checkpoint"
  val inputDir = new File("./generated-data/")

  val maxEventsPerInterval = 20
  val intervalSeconds = 10
  val botDetectionInterval = Seconds(intervalSeconds)

  val maxEventDelay = Seconds(100)
  val blacklistTtl = Minutes(10)
  val maxDetectionDelay = Seconds(60)

  val blacklistTtlSeconds: Int = (blacklistTtl.milliseconds / 1000).toInt
  val retentionSeconds: Int = ((maxEventDelay + botDetectionInterval).milliseconds / 1000).toInt

  /** Initialize the spark session and call a block over it
   */
  def withSpark(appName: String)(block: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()
    try block(spark) finally spark.close()
  }

}
