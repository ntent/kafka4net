package com.ntent.kafka

import java.io._
import java.security.MessageDigest
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, Executors}

import kafka.consumer.{Consumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.Source
import scala.util.Random

/**
  * Created by vchekan on 4/29/2016.
  */
object main extends App {
  val topic = args(0)
  val compression = args(1)
  val action = args(2)
  val hashFileName = "C:\\projects\\kafka4net\\vagrant\\files\\hashes.txt"
                      //"/vagrant/files/hashes.txt"
  val sizesFileName = "C:\\projects\\kafka4net\\vagrant\\files\\sizes.txt"
                      //"/vagrant/files/sizes.txt"
  val md5 = MessageDigest.getInstance("MD5")

  action match {
    case "produce" => produce()
    case "consume" => consume()
    case _ => throw new RuntimeException("Unknown action. Should be 'produce' or 'consume'")
  }

  def produce() = {
    val hashFile = new PrintWriter(new File(hashFileName))
    val sizes = readSizes

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
    println(s"Read ${sizes.length} lines")
    val futures = sizes.map(size => {
      producer.send(generateMessage(size, hashFile))
    })

    producer.close()

    futures.foreach(f => {
      val res = f.get()
      println(s"Sent part:${res.partition()} offset: ${res.offset()}")
    })


    hashFile.close()
  }

  def consume(): Unit = {


    val hashFile = new PrintWriter(new File(hashFileName))
    val consumer = Consumer.create(new ConsumerConfig(consumerProps))
    val consumerMap = consumer.createMessageStreams(Map(topic -> 3))
    val streams = consumerMap.get(topic).get
    val expectedCount = readSizes.length
    val count = new AtomicInteger()

    val threads = for(stream <- streams) yield {
      new Thread(new Runnable {
        override def run(): Unit = {
          try {
            println(s"Starting stream ${stream}")
            val it = stream.iterator()
            while (it.hasNext()) {
              try {
                val msg = it.next()
                val value = msg.message()
                count.incrementAndGet()
                println(s"Got ${count.get()}/$expectedCount size: ${value.length} part: ${msg.partition} offset: ${msg.offset}")
                val hash = md5.digest(value).map("%02X".format(_)).mkString
                hashFile.println(hash)
                if (count.get() == expectedCount) {
                  consumer.shutdown()
                }
                //consumer.commitOffsets(true)
              } catch {
                case e: Throwable => println(e.getMessage)
              }
            }
          } catch {
            case e: Throwable => println(s"Error: ${e.getMessage}")
          }
        }
      })
    }

    threads.foreach(_.start())

    threads.foreach(_.join(5*60*1000))

    hashFile.close()
  }

  def generateMessage(size: Int, hashFile: PrintWriter): ProducerRecord[Array[Byte], Array[Byte]] = {
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

  def consumerProps: Properties = {
    val props = new Properties()

    //props.put("bootstrap.servers", "192.168.56.10:9092")
    props.setProperty("zookeeper.connect", "192.168.56.2:2181")
    props.setProperty("group.id", "kafka4net-testing")
    //props.setProperty("auto.offset.reset", "largest")
    props.setProperty("fetch.message.max.bytes", (500*1024*1024).toString)
    props.put("auto.commit.enable", "false")
    //props.put("auto.commit.interval.ms", "5000");
    //props.put("session.timeout.ms", "30000")
    //props.setProperty("auto.offset.reset", "largest")

    //props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    //props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    //props.setProperty("partition.assignment.strategy", "range")


    //
    //

    /*

    */

    //new ConsumerConfig(props)
    props
  }

  def readSizes = {
    Source.fromFile(sizesFileName).getLines().map(Integer.parseInt(_)).toArray
  }
}
