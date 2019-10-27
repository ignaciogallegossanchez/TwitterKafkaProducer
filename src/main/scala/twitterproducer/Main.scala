package twitterproducer

object Main {
  def main(args: Array[String]): Unit = {
    println("Twitter Kafka Producer")

    val keepCodingReadWriter = new KeepCodingReadWriter(
      in = new TwitterReader(
        termsToTrack = List("#bigdata", "#keepcoding", "datos", "bigdata", "developer")
      ),
      out = new SparkProducer(
        servers = "localhost:9092",
        defaultTopic = "keepcoding"
      ).addEncryption(
        new AESEncryption(Array[Byte]('s','E','c','R','e','T','c','L','0','a','c','a','l','a','n','d')))
    )

    keepCodingReadWriter.start()
  }
}
