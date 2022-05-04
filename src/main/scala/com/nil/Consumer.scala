package com.kafka

import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.immutable._
import org.apache.spark.sql.functions.col

object Consumer {
  def main(args: Array[String]): Unit = {
    consumeFromKafka("quick-start")
  }
  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "13")
    val consumer: KafkaConsumer[String, Map[String,Any]] = new KafkaConsumer[String, Map[String,Any]](props)
    consumer.subscribe(util.Arrays.asList("test"))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator){
        println(data.key()+"--->"+data.value())
        
      }
    }
  }
}