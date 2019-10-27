package twitterproducer

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1


class TwitterReader(termsToTrack :List[String], queueCapacity: Int = 10000){

  // Vars
  private val queue = new LinkedBlockingQueue[String](queueCapacity)
  private var client: BasicClient = null

  // Class init
  init()


  // Function to initialize the class
  private def init(): Unit = {
    if (termsToTrack == null || termsToTrack.isEmpty)
      throw new Exception("termsToTrack cannot be null or empty")

    // Auth
    val authentication = new OAuth1(
      TwitterCredentials.CONSUMERKEY,
      TwitterCredentials.CONSUMERSECRET,
      TwitterCredentials.APITOKEN,
      TwitterCredentials.APITOKENSECRET);

    // What to get from twitter api
    import collection.JavaConverters._
    val endpoint = new StatusesFilterEndpoint();
    endpoint.trackTerms(termsToTrack.asJava)
    endpoint.languages(List("es").asJava)

    // client
    client = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(authentication)
      .endpoint(endpoint)
      .processor(new StringDelimitedProcessor(queue))
      .build();
    client.connect()
  }

  // Checks if client is still ready
  def isReady(): Boolean = {
    !client.isDone
  }

  // Get a string from the input
  def get() = {
    queue.take()
  }
}
