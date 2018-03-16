package kafka.twitter

import com.twitter.bijection.avro.SpecificAvroCodecs.{toBinary, toJson}
import com.typesafe.config.ConfigFactory
import kafka.twitter.TwitterConnector.OnTweetPosted
import kafka.twitter.avro.Tweet
import twitter4j.{FilterQuery, Status}
import scalaj.http._

object KafkaRestProducerApp {

  private val conf = ConfigFactory.load()

  val filterUsOnly: FilterQuery = new FilterQuery().locations(
    Array(-126.562500,30.448674),
    Array(-61.171875,44.087585))

  case class Config(topic: String = "tweets",
                    url: String = "tweets",
                    debug: Boolean = false)

  def main (args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Kafka Twitter Producer", "1.0.0")

      opt[String]('t', "topic") action { (x, c) =>
        c.copy(topic = x)
      } text "Kafka topic name to produce to it"

      opt[String]('u', "url") action { (x, c) =>
        c.copy(url = x)
      } text "Kafka rest url to post events"

      help("help").text("prints this usage text")
    }
    parser.parse(args, Config()) foreach { config =>
      val twitterStream = TwitterConnector.getStream
      twitterStream.addListener(new OnTweetPosted(s => sendToKafkaRest(toTweet(s), config.topic, config.url)))
      twitterStream.filter(filterUsOnly)
    }
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText)
  }

  private def sendToKafkaRest(t:Tweet, topic: String, url: String) = {
    val tweetEnc = toBinary[Tweet].apply(t)
    println(Http(url + s"/topics/$topic")
      .headers(Seq("Content-Type" -> "application/vnd.kafka.json.v2+json", "Accept" -> "application/vnd.kafka.v2+json"))
      .postData(tweetEnc).asString)
    println(toJson(t.getSchema).apply(t)) 
  }
}
