package MyUtil

import java.io.FileNotFoundException

import scala.collection.mutable.ListBuffer
import scala.io.Source

object FileUtil {
  /**
    * 从配置文件中读取配置，并将各行切分为List[String]格式
    *
    * @param confFile  配置文件名，放置于工程的resources目录下。
    * @param separator 字段分隔符，默认为Tab。
    * @return
    */
  def readConfFile(confFile: String, separator: String = "\t"): List[List[String]] = {
    val filePath = this.getClass.getClassLoader.getResource(confFile).getPath
    if (filePath == null) {
      throw new FileNotFoundException(s"missing file:${confFile}")
    }

    val cols = ListBuffer[List[String]]()
    Source.fromFile(filePath).getLines.foreach {
      line => {
        val fields = line.toString.split(separator, -1).toList
        cols += fields
      }
    }
    cols.toList
  }

}
