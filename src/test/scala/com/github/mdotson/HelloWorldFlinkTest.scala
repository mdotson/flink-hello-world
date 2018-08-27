package com.github.mdotson

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.Matchers._
import org.scalatest._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class HelloWorldFlinkTest extends WordSpec with EmbeddedKafka {
  "runs with embedded kafka on arbitrary available ports" should {

    "work" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        Future {
          new FlinkConsumerProducer(1).main("localhost:" + actualConfig.kafkaPort)
        }
        implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
        implicit val stringSerializer: Serializer[String] = new StringSerializer()

        publishStringMessageToKafka("input", "message")
        val result = consumeNumberMessagesFromTopics(Set("output"), 1, timeout = 20.seconds).get("output").head.head
        result shouldBe "1_MESSAGE"
      }
    }
  }
}
