import java.time.Instant

import scala.util.Try

case class Event(
  eventType: String,
  ip: String,
  eventTime: Instant,
  url: String
)

object Event {

  def eventType = "eventType"
  def ip = "ip"
  def eventTime = "event_time"
  def url = "url"

  def parse(eventType: String, ip: String, timeStr: String, url: String): Try[Event] = {
    for {
      milli <- Try(timeStr.toLong)
    } yield Event(eventType, ip, Instant.ofEpochMilli(milli), url)
  }

}
