import java.time.Instant

case class AggregatedEvent(
  ip: String,
  lastEventTime: Instant,
  lastBotEventSecond: Option[Long]
)

object AggregatedEvent {
  def ip = "ip"
  def lastEventTime = "lastEventTime"
  def lastBotEventSecond = "lastBotEventSecond"
}
