package com.syarif.ao

import com.fasterxml.jackson.annotation.JsonFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import java.util.{ Properties }
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import com.google.protobuf.util.JsonFormat

case class Transcation(json: String, et: Long)

object JsonPrinter {
val jsonPrinter = JsonFormat.printer()
  .omittingInsignificantWhitespace()
  .preservingProtoFieldNames()
  .includingDefaultValueFields()
}

object LogDeserialization extends KeyedDeserializationSchema[Transcation] {
 override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): Transcation = {
   val msg = TransactionLogMessage.parseFrom(message)
   Transcation(JsonPrinter.jsonPrinter.print(msg), msg.getEventTimestamp.getSeconds)
 }
  val format = new java.text.SimpleDateFormat("yyMMddHHmm")
  override def isEndOfStream(p: Transcation): Boolean = (format.format(new java.util.Date()).toInt%5 == 0)
  override def getProducedType(): TypeInformation[Transcation] = createTypeInformation[Transcation]
}

object Consumer {
  private val props = new Properties

  props.setProperty("bootstrap.servers", "127.0.0.1:<bootstrap port>")
  props.setProperty("group.id", "transaction-group-01")
  props.setProperty("client.id", "transaction-client-01")
  props.setProperty("max.poll.records", "100")

 val consumer_es = new FlinkKafkaConsumer010[Transcation]("TRANSACTION-log", LogDeserialization, props)
}

object KafkaPricing {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val source_es = Consumer.consumer_es
    val ds_es: DataStream[Transcation] = env.addSource(source_es)

    while (true){
      Thread.sleep(7200)
      val format = new java.text.SimpleDateFormat("yyMMddHHmmss")
      val current_time = format.format(new java.util.Date())
      val dir_name = current_time.toString

      val upperDirFormat = new java.text.SimpleDateFormat("yyMMdd")
      val upper_current_time = upperDirFormat.format(new java.util.Date())
      val upper_dir_name = upper_current_time.toString
      ds_es.map { p => p.json}
        .writeAsText("/tmp/transaction-log/"+upper_dir_name+'/'+dir_name)
      env.execute()
    }
  }
}
