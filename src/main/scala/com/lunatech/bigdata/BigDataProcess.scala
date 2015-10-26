package com.lunatech.bigdata

import java.io.File
import com.lunatech.bigdata.model.Event
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Process big data in a particular directory.
 */
object BigDataProcess extends App {

  implicit val formats = DefaultFormats
  private val DIR = "/tmp/GitHubArchiveJan"

  // Load file names
  private val fileNames = getFileNames(DIR)

  // Prepare futures
  println(s"Processing ${fileNames.size} files...")
  val futures = createFutures(fileNames)

  private val waiting: Future[List[Map[String, Int]]] = Future.sequence(futures)
  private val ready: Future[List[Map[String, Int]]] = Await.ready(waiting,Duration.Inf)

  ready.onComplete {
    case Success(v) => println(prepareReport(v.flatten.map(x => x._1).groupBy(x => x).map(x => (x._1, x._2.length))))
    case Failure(ex) => println("Error: " + ex.getMessage)
  }
  println("done")

  /**
   * For a given path, get all the json files.
   * @param s path
   * @return List of json file paths
   */
  private def getFileNames(s: String): List[String] = {
    new File(s).listFiles.filter(x => x.isFile && x.getName.endsWith("json")).toList.map(_.getPath)
  }

  /**
   * For every file in the list, create a future to process the file
   * @param files list of file names
   * @return list of futures having a map with the results per file.
   */
  private def createFutures(files: List[String]): List[Future[Map[String, Int]]] = {
    files.map {
      x => Future {
        getContent(x)
      }
    }
  }

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
