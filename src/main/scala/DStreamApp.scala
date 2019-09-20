
import java.time.Instant
import java.util.logging.Logger

import JsonEncoder.JsonEvent
import RollingHistory.{EventSecond, EventsPerSecond}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

class DStreamApp extends App {

  val appName = "DStream Spark App"
  val numKafkaThreads = 1
  val batchInterval = Seconds(1)

  val log = Logger.getLogger(appName)

  startStreaming(appName) { streamingContext =>
    //We expect IP to be a key for a stream
    createKafkaStream(streamingContext)
      .flatMapValues(JsonEvent.parseOpt)
      .updateStateByKey(updateState)
      .mapValues(_.evaluatedEvent)
      .print
  }

  def startStreaming(appName: String)(block: StreamingContext => Unit): Unit = Config.withSpark(appName) { spark =>
    val streamingContext = new StreamingContext(spark.sparkContext, batchInterval)
    streamingContext.checkpoint(Config.checkpointDir)
    block(streamingContext)
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def createKafkaStream(streamingContext: StreamingContext): ReceiverInputDStream[(String, String)] = {
    KafkaUtils.createStream(
      streamingContext,
      Config.zkHost,
      appName,
      Map(Config.kafkaTopic -> numKafkaThreads)
    )
  }

  def updateState(events: Seq[Event], historyOpt: Option[RollingHistory]): Option[RollingHistory] = {
    if (events.isEmpty) {
      historyOpt
    } else if (historyOpt.isEmpty) {
      val newHistory = RollingHistory(events.head, RollingHistory.emptyEvents)
      Some(updateHistory(newHistory, events.tail))
    } else {
      Some(updateHistory(historyOpt.get, events))
    }
  }

  /** Add events to a history */
  def updateHistory(lastHistory: RollingHistory, events: Seq[Event]): RollingHistory = {
    events.foldLeft(lastHistory) { (history, event) =>
      val eventKey = event.eventTime.getEpochSecond
      val eventsPerSecond = addEvent(history.eventsPerSecond, eventKey)
      val isBot = detectBot(eventKey, eventsPerSecond)
      val lastBotEventSecond = if (isBot) {
        Some(eventKey)
      } else {
        filterTtl(history.lastBotEventSecond)
      }
      val status = if (isBot) " MARKED AS A BOT"
        else if (lastBotEventSecond.nonEmpty) s" BLACKLISTED since ${Instant.ofEpochSecond(lastBotEventSecond.get)}"
        else ""
      log.info(s"${event.ip} ${event.eventTime}$status")
      RollingHistory(event, eventsPerSecond, lastBotEventSecond)
    }
  }

  /** Add event key to a event by second map */
  def addEvent(eventsPerSecond: EventsPerSecond, eventKey: EventSecond): EventsPerSecond = {
    val currentCount = eventsPerSecond(eventKey)
    val updatedEvents = if (currentCount < Int.MaxValue) {
      eventsPerSecond.updated(eventKey, currentCount + 1)
    } else {
      eventsPerSecond // Prevent `Int` overflow here
    }
    val compactedEvents = if (updatedEvents.size > Config.retentionSeconds) {
      updatedEvents.drop(updatedEvents.size - Config.retentionSeconds)
    } else {
      updatedEvents
    }
    compactedEvents
  }

  /** Check nearby region for a new key to detect if there are too much requests for it */
  def detectBot(eventKey: EventSecond, eventsPerSecond: EventsPerSecond): Boolean = {
    // Inspect time intervals of n seconds. n = BotDetection.inspectedSeconds
    // Reminder: event key is time of an event in seconds
    val left = eventKey - Config.intervalSeconds
    val right = eventKey + Config.intervalSeconds
    // Count events in n seconds before the new event inclusive
    // Use `Long` to prevent overflow
    var eventsPerInterval: Long = (left to eventKey).map(eventsPerSecond(_).toLong).sum
    var cur = eventKey + 1
    while (cur <= right && !isBot(eventsPerInterval)) {
      // Count events in n seconds starting with cur
      eventsPerInterval += eventsPerSecond(cur)- eventsPerSecond(cur - eventKey + left)
      cur += 1
    }
    isBot(eventsPerInterval)
  }

  def isBot(eventsPerInterval: Long): Boolean = {
    eventsPerInterval > Config.maxEventsPerInterval
  }

  def filterTtl(lastBotSecond: Option[EventSecond]): Option[EventSecond] = {
    val ttlThreshold = (System.currentTimeMillis() - Config.blacklistTtl.milliseconds) / 1000
    lastBotSecond.filter(_ > ttlThreshold)
  }

}
