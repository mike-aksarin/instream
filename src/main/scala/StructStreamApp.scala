import java.time.Instant
import java.util.logging.Logger

import EventEncoder._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

object StructStreamApp extends App {

  val appName = "Structured Stream Spark App"
  def delayThreshold = s"${Config.retentionSeconds} seconds"
  def windowDuration = s"${Config.intervalSeconds} seconds"
  def slideDuration = "1 second"

  val prevBotEventSecondColumn = "prevBotSecondColumn"
  val log = Logger.getLogger(appName)

  startStreaming(appName) { unboundedEvents =>
    val key = col(Event.ip)
    val eventTime = col(Event.eventTime)
    val events = unboundedEvents.withWatermark(Event.eventTime, delayThreshold)
    val ttlWindow = Window
      .partitionBy(key)
      .orderBy(AggregatedEvent.lastBotEventSecond)
      .rangeBetween(-Config.blacklistTtlSeconds, Window.currentRow)
    val processedEvents = events
      .groupBy(key, window(eventTime, windowDuration, slideDuration))
      .agg(key, min(eventTime).as("start"), max(eventTime).as("end"), count("*"))
      .map(detectBot)
      .withColumn(prevBotEventSecondColumn, max(AggregatedEvent.lastBotEventSecond).over(ttlWindow))
      .map(processBlacklistTtl)
    events
      .join(processedEvents, events(Event.ip) === processedEvents(AggregatedEvent.ip))
      .map(parseEvaluatedEvent)
  }

  def parseEvaluatedEvent(row: Row): EvaluatedEvent = {
    EvaluatedEvent(
      Event(
        row.getAs[String](Event.eventType),
        row.getAs[String](Event.ip),
        row.getAs[java.sql.Date](Event.eventTime).toInstant,
        row.getAs[String](Event.url)
      ),
      row.getAs[Option[Long]](AggregatedEvent.lastBotEventSecond).nonEmpty
    )
  }

  def detectBot(row: Row): AggregatedEvent = {
    val ip = row.getAs[String](Event.ip)
    val start = row.getAs[java.sql.Date]("start")
    val end = row.getAs[java.sql.Date]("end")
    val count = row.getAs[Int]("count")
    val isBot = count > Config.maxEventsPerInterval
    val lastEventTime = end.toInstant
    val lastBotEventSecond = if (isBot) Some(lastEventTime.getEpochSecond) else None
    log.info(s"$ip $start $end $count ${if (isBot)" MARKED AS A BOT"}")
    AggregatedEvent(ip, lastEventTime, lastBotEventSecond)
  }

  def processBlacklistTtl(row: Row): AggregatedEvent = {
    val ip = row.getAs[String](AggregatedEvent.ip)
    val lastEventTime = row.getAs[java.sql.Date](AggregatedEvent.lastEventTime).toInstant
    val botEventSecond = row.getAs[Option[Long]](AggregatedEvent.lastBotEventSecond)
    val prevBotEventSecond = row.getAs[Option[Long]](prevBotEventSecondColumn)
    val lastBotEventSecond = botEventSecond.orElse(filterTtl(prevBotEventSecond))
    if (lastBotEventSecond.nonEmpty && botEventSecond.isEmpty) {
      log.info(s"$ip $lastEventTime BLACKLISTED since ${Instant.ofEpochSecond(lastBotEventSecond.get)}")
    }
    AggregatedEvent(ip, lastEventTime, lastBotEventSecond)
  }

  def filterTtl(lastBotSecond: Option[Long]): Option[Long] = {
    val ttlThreshold = (System.currentTimeMillis() - Config.blacklistTtl.milliseconds) / 1000
    lastBotSecond.filter(_ > ttlThreshold)
  }

  def startStreaming(appName: String)(block: Dataset[Event] => Unit): Unit = Config.withSpark(appName) { spark =>
    val dataFrame = spark
      .readStream
      //TODO: Kafka stream config here
      .load()
      .as[Event]
    block(dataFrame)
  }

}
