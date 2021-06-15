package MyUtil.ScalaUtil

import java.util.Properties

import scala.collection.mutable
/**
  * @ Author     ：javazyh.
  * @ Date       ：Created in 2019-12-${DAT}-15:57
  * @ Description：${description}
  * @ Modified By：
  *
  * @Version: $version$
  */
object PropertiesUtil {
    var map: mutable.Map[String, Properties] =mutable.Map[String,Properties]()

    def getProperty(confFile:String,propName:String)={
        map.getOrElseUpdate(confFile,{
            val is = ClassLoader.getSystemResourceAsStream(confFile)
            val ps = new Properties()
            ps.load(is)
            ps
        }).getProperty(propName)
    }

//    def main(args: Array[String]): Unit = {
//        println(getProperty("config.properties", "kafka.broker.list"))
//    }
}
