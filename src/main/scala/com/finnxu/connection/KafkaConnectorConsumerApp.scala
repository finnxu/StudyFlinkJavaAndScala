package com.finnxu.connection

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
 * PackageName : com.finnxu.connection
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-21 22:37
 * Description : TODO
 */
object KafkaConnectorConsumerApp {

  private def kafkaCOnnector(env: StreamExecutionEnvironment): Unit = {
    val topic = ""
    val properties = new Properties()
    env.addSource(new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), properties))
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    kafkaCOnnector(env)
    env.execute("KafkaConnectorConsumerApp")
  }
}
