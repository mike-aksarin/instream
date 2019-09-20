import java.time.Instant
import java.util.logging.{Level, Logger}
import io.circe._
import io.circe.parser._
import io.circe.generic.semiauto._
import io.circe.syntax._
import cats.syntax.either._

object JsonEncoder {

  val log = Logger.getLogger("JsonEncoder")

  implicit val instantEncoder: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)
  implicit val instantDecoder: Decoder[Instant] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(Instant.parse(str)).leftMap(t => "Instant" + t.getMessage)
  }

  implicit val eventEncoder: Encoder[JsonEvent] = deriveEncoder[JsonEvent]
  implicit val eventDecoder: Decoder[JsonEvent] = deriveDecoder[JsonEvent]
  implicit val evaluatedEventEncoder: Encoder[JsonEvaluatedEvent] = deriveEncoder[JsonEvaluatedEvent]

  def logError(json: String)(e: Throwable): Option[Nothing] = {
    log.log(Level.WARNING, s"Could not parse $json", e)
    None
  }

  case class JsonEvent(
    `type`: String,
    ip: String,
    event_time: Instant,
    url: String
  ) {
    def toEvent: Event = Event(`type`, ip, event_time, url)
    def toJson: Json = this.asJson
  }

  object JsonEvent {

    def apply(event: Event): JsonEvent = {
      import event._
      apply(eventType, ip, eventTime, url)
    }

    def toResult(jsonEvent: JsonEvent): Option[Event] = {
      Option(jsonEvent.toEvent)
    }

    def parseOpt(json: String): Option[Event] = {
      parse(json).fold(logError(json), toResult)
    }

    def parse(json: String): Either[Error, JsonEvent] = {
      decode[JsonEvent](json)
    }
  }

  case class JsonEvaluatedEvent(
    `type`: String,
    ip: String,
    event_time: Instant,
    url: String,
    is_bot: Boolean
  ) {
    def toJson: Json = this.asJson
  }

  object JsonEvaluatedEvent {
    def apply(evaluatedEvent: EvaluatedEvent): JsonEvaluatedEvent = {
      import evaluatedEvent.event._
      import evaluatedEvent.isBot
      apply(eventType, ip, eventTime, url, isBot)
    }
  }

}
