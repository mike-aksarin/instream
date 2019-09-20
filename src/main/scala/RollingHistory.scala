import RollingHistory._

/** Stores amounts of events per seconds.
  * Intended to be a state for a DStream stateful algorithm
  */
case class RollingHistory(currentEvent: Event,
                          eventsPerSecond: EventsPerSecond = emptyEvents,
                          lastBotEventSecond: Option[EventSecond] = None) {
  def isBot: Boolean = lastBotEventSecond.nonEmpty
  def evaluatedEvent = EvaluatedEvent(currentEvent, isBot)
}

object RollingHistory {

  /** Time of an event in seconds */
  type EventSecond = Long // maybe we can have smaller memory footprint for `EventSecond = Int`

  /** Number of events for each second */
  type EventsPerSecond = Map[EventSecond, Int]

  val emptyEvents: EventsPerSecond = Map().withDefaultValue(0)

}