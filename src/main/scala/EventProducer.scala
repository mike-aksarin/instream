import java.io.{BufferedWriter, File, FileWriter}
import java.net.InetAddress
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZonedDateTime}
import java.util.{Timer, TimerTask}

import JsonEncoder.JsonEvent

import scala.util.Random

object EventProducer extends App {

  val period = 3000L

  runTimer(generateEventFile())
  //generateEventFile()

  def runTimer(task: => Unit):Unit = {
    val timer = new Timer()
    val timerTask = new TimerTask { override def  run(): Unit = task }
    timer.schedule(timerTask, 0L, period)
    // The task could be stopped via `timerTask.cancel()`
  }

  def generateEventFile(): Unit = {
    val fileName = s"${System.currentTimeMillis}.json"
    val json = JsonEvent(produceEvent).toJson
    writeFile(fileName, json.toString)
  }

  def produceEvent: Event = {
    val ip = randomIp
    Event(eventType, ip.getHostAddress, randomTime, ip.toString)
  }

  def eventType: String = {
    "click"
  }

  def randomIp: InetAddress = {
    val bytes = new Array[Byte](4)
    Random.nextBytes(bytes)
    InetAddress.getByAddress(bytes)
  }

  def randomTime: Instant = {
    val delay = Random.nextInt(Config.maxEventDelay.milliseconds.toInt)
    ZonedDateTime.now().minus(delay, ChronoUnit.MILLIS).toInstant
  }

  def writeFile(fileName: String, text: String): Unit = {
    Config.inputDir.mkdirs()
    val file = new File(Config.inputDir, fileName)
    val writer = new BufferedWriter(new FileWriter(file))
    try {
      writer.write(text)
    } finally {
      writer.close()
    }
  }

}
