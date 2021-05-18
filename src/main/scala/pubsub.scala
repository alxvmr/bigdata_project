import org.apache.log4j.{Level, Logger}
import com.google.cloud.pubsub.v1.{Publisher}
import com.google.protobuf.{ByteString}
import com.google.pubsub.v1.PubsubMessage

object pubsub {
  case class Event(NOC: String, year: String, City: String, Medal: String)
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    //*******************************************************************

    val projectID = "myproject-310015"
    val subscription = "sport-sub"
    val topicId = "projects/myproject-310015/topics/sport"


    val publisher = Publisher
      .newBuilder(topicId)
      .build()

      while(true) {
        //for (counter <- 0 to 100) {
          val event = generator.generate()
          val mess = event.toString()

          val data = ByteString
            .copyFromUtf8(mess)
          val pubsubMessage = PubsubMessage
            .newBuilder()
            .setData(data)
            .build()
          val messageIdFuture = publisher
            .publish(pubsubMessage)
          val messageId = messageIdFuture
            .get()
          print(" Published message ID: " + messageId + "\n")
       // }
      }
      throw new Exception("How did I end up here?")
//******************************************************************************************************************
  }
}