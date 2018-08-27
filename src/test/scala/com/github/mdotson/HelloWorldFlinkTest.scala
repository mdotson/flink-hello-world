package com.github.mdotson

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.Matchers._
import org.scalatest._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class HelloWorldFlinkTest extends WordSpec with EmbeddedKafka {
  "runs with embedded kafka on arbitrary available ports" should {

    "work" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        Future {
          val fcp1 = new FlinkConsumerProducer(1).main("localhost:" + actualConfig.kafkaPort)
        }
        implicit val stringDeserializer = new StringDeserializer()
        implicit val stringSerializer = new StringSerializer()

        publishStringMessageToKafka("input", "message")
        val result = consumeFirstStringMessageFrom("output")
        result shouldBe "1_MESSAGE"
      }
    }
  }
}
