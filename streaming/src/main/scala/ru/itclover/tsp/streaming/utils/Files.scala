package ru.itclover.tsp.streaming.utils

import ru.itclover.tsp.streaming.utils.CollectionOps.TryOps

import java.io.{File, FileWriter}
import scala.util.Try

object Files:

  def writeToFile(path: String, content: String, overwrite: Boolean = false): Try[Unit] =
    val pw = new FileWriter(new File(path), !overwrite)
    Try {
      pw.write(content)
    }.eventually:
      pw.close()

  def readResource(resourcePath: String): Iterator[String] =
    val stream = getClass.getResourceAsStream(resourcePath)
    scala.io.Source.fromInputStream(stream).getLines

  def readFile(path: String): Try[String] = for
    src <- Try(scala.io.Source.fromFile(path))
    str <- Try(src.mkString).eventually:
      src.close
  yield str

  def rmFile(path: String): Boolean =
    new File(path).delete()
