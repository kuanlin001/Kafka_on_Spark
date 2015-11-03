import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.util.Properties

object Data_Producer {
  def main(args: Array[String]) {
    println("Starting data-generation simulation")
    
    val sparkConf = new SparkConf().setAppName("DataProducer")
    .setMaster("local[4]")    // comment out when submitting to spark cluster
    val sc = new SparkContext(sparkConf)
    
    sc.parallelize(Array(1,2,3,4)).foreach(x => {
      val producer = newStringKafkaProducer
      sendMsg(producer=producer, topic="test", value="[server " + x + "] msg1")
      sendMsg(producer=producer, topic="test", value="[server " + x + "] msg2")
      sendMsg(producer=producer, topic="test", value="[server " + x + "] msg3")
      producer.close()
    })
    
    //sendMsg(topic="test-topic", value="some very important message")
    
    println("Data-generation simulation ends")
  }
  
  def sendMsg(producer: KafkaProducer[String, String] = null, topic: String, value: String):  KafkaProducer[String, String] = {
    var prod: KafkaProducer[String, String] = null
    if(producer == null)
      prod = newStringKafkaProducer
    else
      prod = producer
    
    val rec = new ProducerRecord[String, String](topic, value)
    prod.send(rec)
    return prod
  }
  
  def newStringKafkaProducer: KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", "localhost:9092")
    props.put("metadata.broker.list", "localhost:9092")
    val producer = new KafkaProducer[String,String](props)
    return producer
  }
}

