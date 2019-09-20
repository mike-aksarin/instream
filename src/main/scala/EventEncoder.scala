import org.apache.spark.sql.{Encoder, Encoders, Row}

object EventEncoder {
  implicit val eventEncoder: Encoder[Event] = Encoders.product[Event]
  implicit val aggregatedEventEncoder: Encoder[AggregatedEvent] = Encoders.product[AggregatedEvent]
  implicit val evaluatedEventEncoder: Encoder[EvaluatedEvent] = Encoders.product[EvaluatedEvent]
  implicit val stringBooleanEncoder: Encoder[(String, Boolean)] = Encoders.product[(String, Boolean)]
}
