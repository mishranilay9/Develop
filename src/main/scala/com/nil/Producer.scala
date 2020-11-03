package com.kafka

import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.util.parsing.json.JSON
object Producer {
  def main(args: Array[String]): Unit = {
    writeToKafka("quick-start")
  }
  def writeToKafka(topic: String): Unit = {
    val jsonstring =
        s"""{
            | "id": "0001",
            | "name": "Peter"
            |}
         """.stripMargin
    //val jsonMap: Map[String, Any] = JSON.parseFull(jsonstring).get.asInstanceOf[Map[String, Any]]
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String]("test", "key", jsonstring)
    producer.send(record)
    producer.close()
  }
}