package com.lunatech.bigdata

import java.io.File
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Process big data in a particular directory.
 */
object BigDataProcess {

  implicit val formats = DefaultFormats
  private val PATH = "/tmp/GitHubArchiveJan"

  def main(args: Array[String]): Unit = {
    // Load file names
    val filePaths = getFilePaths(PATH)

    // Map every file with a map containing the events and no of occurrences, as a future.
    val allEventsInFiles: Seq[Future[Map[String, Int]]] = filePaths.map(x => getEvents(x))
    val futureSeq: Future[Seq[Map[String, Int]]] = Future.sequence(allEventsInFiles)

    //When future is completed, will return a sequence of maps, one map per file.
    val filesResults = Await.result(futureSeq, Duration.Inf)
    val report = filesResults.flatten.groupBy(x => x._1).mapValues(_.map(_._2).sum)

    println("Report:")
    println(prepareReport(report))

  }

  /**
   * For a given file path, extract all the events and create a Map with the event name and number of occurrences.
   * @param filePath
   * @return Future of map with event names and occurrences
   */
  private def getEvents (filePath: String): Future[Map[String, Int]] = Future {
    println(s"Working on file $filePath")
    Source.fromFile(filePath.toString).getLines().toList
      .map(line => (parse(line.toString) \ "type").extract[String]) //extract event types directly per line
      .groupBy(x => x)
      .map(event => (event._1, event._2.length)) //counting how many events per type
  }

  /**
   * For a given path, get all the json files.
   * @param s path
   * @return List of json file paths
   */
  private def getFilePaths(s: String): Seq[String] = {
    new File(s).listFiles.filter(x => x.isFile && x.getName.endsWith("json")).toSeq.map(_.getPath)
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
