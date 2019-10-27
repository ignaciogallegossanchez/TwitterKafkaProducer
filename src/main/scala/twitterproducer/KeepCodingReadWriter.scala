package twitterproducer


class KeepCodingReadWriter(
    in: TwitterReader,
    out: SparkProducer) {

  def start() = {
    while(in.isReady()) {
      out.write(in.get())
    }
  }
}
