package kafka.twitter

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs.{toBinary, toJson}
import com.typesafe.config.ConfigFactory
import kafka.twitter.TwitterConnector.OnTweetPosted
import kafka.twitter.avro.Tweet
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j.{FilterQuery, Status}

object KafkaProducerApp {

  private val conf = ConfigFactory.load()

  val kafkaProducer: KafkaProducer[String, Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaProducer[String, Array[Byte]](props)
  }

  val filterUsOnly: FilterQuery = new FilterQuery().locations(
    Array(-126.562500,30.448674),
    Array(-61.171875,44.087585))

  case class Config(topic: String = "tweets",
                    debug: Boolean = false)

  def main (args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Kafka Twitter Producer", "1.0.0")

      opt[String]('t', "topic") action { (x, c) =>
        c.copy(topic = x)
      } text "Kafka topic name to produce to it"

      opt[Unit]("debug").hidden().action{ (_, c) =>
        c.copy(debug = true) } text "Option to print tweets on screen"

      help("help").text("prints this usage text")
    }
    parser.parse(args, Config()) foreach { config =>

      val twitterStream = TwitterConnector.getStream
      twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s), config.topic, config.debug)))
      twitterStream.filter(filterUsOnly)
    }
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText)
  }

  private def sendToKafka(t:Tweet, topic: String, debug: Boolean) {
    if (debug) println(toJson(t.getSchema).apply(t))
    val tweetEnc = toBinary[Tweet].apply(t)
    val msg = new ProducerRecord[String, Array[Byte]](topic, tweetEnc)
    kafkaProducer.send(msg)
  }
}
