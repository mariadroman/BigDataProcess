package com.lunatech.bigdata

import java.io.File
import scala.io.Source

/**
 * Process big data in a particular directory.
 */
object BigDataProcess extends App {

  private val DIR = "/Documents/LogsGitHub"

  // Load file names
  val fileNames = new File(DIR).listFiles.filter(_.getName.endsWith("json")).toList

  //Extract data
  println("Processing files...")
  val types = for {
    name <- fileNames
  } yield {
    val content = getEvents(getContent(name.getPath)).groupBy(x => x).map(a => (a._1, a._2.length))
    content
  }

  // Prepare results
  private val result: Map[String, Int] = types.flatten.groupBy(x => x._1).map(a => (a._1, a._2.map(_._2).sum))
  println("Printing report...")
  println(prepareReport(result))

  /**
   * Extract content of a file.
   * @param fileName file name
   * @return List of string containing one line of the file per element
   */
  def getContent(fileName: String): List[String] = {
    val file = Source.fromFile(s"$fileName")
    val content = file.getLines().toList
    file.close()
    content
  }

  /**
   * Extract the events' name
   * @param content information in JSON format
   * @return List of events' occurrence in the content
   */
  def getEvents(content: List[String]): List[String] = {
    for {
      line <- content
      tokens <- line.split(",")
      if tokens.startsWith("\"type\":")
      token <- tokens.split(":")
      if token.endsWith("Event\"")
      eventType <- token.split(":")
    } yield eventType
  }

  /**
   * Format the report to be printed easy to read
   * @param inf information to be formatted
   * @return String properly formatted for printing
   */
  def prepareReport(inf: Map[String, Int]): String = {
    val maxLeftSpaces = inf.map(_._1.length).max
    val maxRightSpaces = inf.map(_._2.toString.length).max

    val r = inf.toSeq.sortBy(_._1).toList

    val report = for {
      m <- r
    } yield (" " * (maxLeftSpaces - m._1.length)) + m._1  + " | " + (" " * (maxRightSpaces - m._2.toString.length)) + m._2

    report.mkString("\n")
  }

}


