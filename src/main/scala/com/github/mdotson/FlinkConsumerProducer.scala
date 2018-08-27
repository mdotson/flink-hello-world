package com.github.mdotson

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

class FlinkConsumerProducer(val id: Int) extends Serializable {

  def main(bootstrapServers: String): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val kafkaConsumerProperties = Map(
      "group.id" -> "flink",
      "bootstrap.servers" -> bootstrapServers,
      "auto.offset.reset" -> "earliest"
    )

    val kafkaConsumer = new FlinkKafkaConsumer011[String](
      "input",
      KafkaStringSchema,
      kafkaConsumerProperties
    )

    val kafkaProducer = new FlinkKafkaProducer011[String](
      bootstrapServers,
      "output",
      KafkaStringSchema
    )

    val lines = env.addSource(kafkaConsumer)

    val wordCounts = lines
      .map(word => word.toUpperCase())
      .map(word => s"${id}_$word")
      .addSink(kafkaProducer)

    env.execute()
  }

  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
  }

  object KafkaStringSchema extends SerializationSchema[String] with DeserializationSchema[String] {

    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flink.api.java.typeutils.TypeExtractor

    override def serialize(t: String): Array[Byte] = t.getBytes("UTF-8")

    override def isEndOfStream(t: String): Boolean = false

    override def deserialize(bytes: Array[Byte]): String = new String(bytes, "UTF-8")

    override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  }

}
