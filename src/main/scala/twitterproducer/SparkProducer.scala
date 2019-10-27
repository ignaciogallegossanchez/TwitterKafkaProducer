package twitterproducer

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}



class SparkProducer(servers:String = "localhost:9092", defaultTopic: String) {

  private val producer = createProducer
  private var cipher: AESEncryption = null

  private def createProducer() = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    properties.put(ProducerConfig.ACKS_CONFIG, "1")
    properties.put(ProducerConfig.LINGER_MS_CONFIG, new Integer(500))
    properties.put(ProducerConfig.RETRIES_CONFIG, new Integer(0))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    new KafkaProducer[Long,String](properties)
  }

  def stop() = {
    producer.close()
  }

  def addEncryption(encrypt: AESEncryption): SparkProducer = {
    this.cipher = encrypt
    this
  }

  def write(message: String, topic: String = defaultTopic ): Unit = {
    import org.apache.kafka.clients.producer.ProducerRecord
    import java.lang.System.currentTimeMillis

    var messageToSend: String = if (cipher!= null) {
      cipher.encrypt(message)
    } else {
      message
    }

    val record = new ProducerRecord[Long, String](topic, currentTimeMillis(), messageToSend)
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null){
          println(s"Error while sending to writer: ${exception}")
        }
      }
    })
  }
}
