package nycpipeline

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
/**
 * @author shahroz
 */
class DynamicReceiver(url: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    new Thread("Dynamic Data Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {}

  private def receive() = {

    while (!isStopped()) {

      val dynamicDataResult = scala.io.Source.fromURL(url).mkString

      store(dynamicDataResult)

    }
  }

}