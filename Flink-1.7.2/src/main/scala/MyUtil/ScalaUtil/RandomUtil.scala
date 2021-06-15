package MyUtil.ScalaUtil

import scala.util.Random

object RandomUtil {
  /**
    * 生成n位随机数
    * @param length
    * @return
    */
  def getRandom (length:Int): String ={
    var result=""
   val random = new Random()
    for( i<- 0 to length){
      result+=random.nextInt(10).toString
    }
    result
  }

}
