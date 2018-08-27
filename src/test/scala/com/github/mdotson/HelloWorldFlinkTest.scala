package com.github.mdotson

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.Matchers._
import org.scalatest._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class HelloWorldFlinkTest extends WordSpec with EmbeddedKafka {
  "runs with embedded kafka on arbitrary available ports" should {

    "work" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        Future {
          val fpp = FlinkExample.main("localhost:" + actualConfig.kafkaPort)
        }
        implicit val stringDeserializer = new StringDeserializer()
        implicit val stringSerializer = new StringSerializer()
//        createCustomTopic("input")
        createCustomTopic("output")
        // now a kafka broker is listening on actualConfig.kafkaPort
        publishStringMessageToKafka("input", "message")
        val result = consumeFirstStringMessageFrom("output")
        result shouldBe "MESSAGE"
      }
    }
  }
}
