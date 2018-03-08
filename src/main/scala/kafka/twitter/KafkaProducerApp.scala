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

  val KafkaTopic = "tweets"

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


  def main (args: Array[String]) {
    val twitterStream = TwitterConnector.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s))))
    twitterStream.filter(filterUsOnly)
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText)
  }

  private def sendToKafka(t:Tweet) {
    println(toJson(t.getSchema).apply(t))
    val tweetEnc = toBinary[Tweet].apply(t)
    val msg = new ProducerRecord[String, Array[Byte]](KafkaTopic, tweetEnc)
    kafkaProducer.send(msg)
  }
}
