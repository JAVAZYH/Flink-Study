package MySource

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction

class DubiousSource  extends SourceFunction[String]{
  var isRunning: Boolean = true
  val datas: List[String] = List("1534280187", "1534280186", "333", "444")
  private val random = new Random()
  var number: Long = 0

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (isRunning) {
      val index: Int = random.nextInt(4)
      val value: Int = random.nextInt(10)
      ctx.collect(datas(index)+"|"+s"2020-06-30 22:11:5${value}")
      number += 1
      Thread.sleep(500)
      if(number==5){
        cancel()
      }
    }

  }
  override def cancel(): Unit = {
    isRunning = false
  }

}
