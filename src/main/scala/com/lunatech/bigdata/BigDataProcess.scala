package com.lunatech.bigdata

import java.io.File
import com.lunatech.bigdata.model.Event
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.io.Source

/**
 * Process big data in a particular directory.
 */
object BigDataProcess extends App {

  implicit val formats = DefaultFormats
  private val DIR = "/tmp/GitHubArchiveJan"

  // Load file names
  private val files = new File(DIR).listFiles.filter(_.getName.endsWith("json")).toList
  private val fileNames = files.map(_.getPath)

  // Process files
  println(s"Processing ${fileNames.size} files...")
  private val types = fileNames.flatMap(x => getContent(x)).groupBy(x => x._1).mapValues(_.map(_._2).sum)

  // Prepare and display results
  println("Printing report...")
  println(prepareReport(types))

  /**
   * Given file name, search all event occurrences
   * @param fileName file name
   * @return Map with all events and occurrences
   */
  def getContent(fileName: String): Map[String, Int] = {
    val file = Source.fromFile(s"$fileName")
    val content = file.getLines().toList
    val eventsPerFile = content.map(x =>
      parse(x.toString.replace("type", "evType").replace("public", "evPublic").replace("created_at", "evCreated"))
        .extract[Event]
        .evType)
      .groupBy(x => x)
      .map(x => (x._1, x._2.length))
    file.close()
    eventsPerFile
  }

  /**
   * Given a map of events and their number of occurrences, provide an easy-to-read information string
   * @param inf information to be formatted
   * @return String properly formatted for printing
   */
  def prepareReport(inf: Map[String, Int]): String = {
    val maxLeftSpaces = inf.map(_._1.length).max
    val maxRightSpaces = inf.map(_._2.toString.length).max
    val r = inf.toSeq.sortBy(_._2).toList
    val report = for {
      m <- r
    } yield (" " * (maxLeftSpaces - m._1.length)) + m._1 + " | " + (" " * (maxRightSpaces - m._2.toString.length)) + m._2
    report.mkString("\n")
  }

}
