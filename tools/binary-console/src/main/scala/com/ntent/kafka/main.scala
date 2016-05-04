package com.ntent.kafka

import java.io._
import java.security.MessageDigest
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.Source
import scala.util.Random

/**
  * Created by vchekan on 4/29/2016.
  */
object main extends App {
  val compression = args(0)
  val topic = "topic11.java.compressed"
  val sizes = readSizes
  val hashFile = new PrintWriter(new File("/vagrant/files/hashes.txt"))
  val md5 = MessageDigest.getInstance("MD5")

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
  println(s"Read ${sizes.length} lines")
  val futures = sizes.map(size => {
    producer.send(generateMessage(size))
  })

  producer.close()

  futures.foreach(f => {
    val res = f.get()
    println(s"Sent part:${res.partition()} offset: ${res.offset()}")
  })


  hashFile.close()

  def generateMessage(size: Int): ProducerRecord[Array[Byte], Array[Byte]] = {
    val buff = new Array[Byte](size)
    Random.nextBytes(buff)
    val hash = md5.digest(buff).map("%02X".format(_)).mkString
    hashFile.println(hash)
    //System.out.println(hash)

    // put all messages into the same partition
    val key = Array[Byte](0)
    println(hash)
    new ProducerRecord(topic, key, buff)
  }

  def producerProps = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.10:9092")
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10000")
    props.setProperty(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, (11*1024*1024).toString) // 11Mb
    props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, (8*1000*1000).toString)
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (1024*1024).toString)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props
  }

  def readSizes = {
    Source.fromFile("/vagrant/files/sizes.txt").getLines().map(Integer.parseInt(_)).toArray
  }
}
