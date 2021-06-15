package MySource

import java.util.Random

import MyUtil.TimeUtil
import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * id,
  * name
  * logtime
  */
  class StudentSource extends SourceFunction[String] {
    var isRunning: Boolean = true
    val ids: List[String] = List("111", "222", "333", "444")
    val names:List[String]=List("张,三","李,三,四","王,五","赵,六")
    private val random = new Random()
    var number: Long = 0

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while (isRunning) {
        ctx.collect(
          ids(random.nextInt(4))+"|"+
          names(random.nextInt(4))+"|"+
          TimeUtil.timestampToString(System.currentTimeMillis())
        )

        number += 1
        Thread.sleep(500)
        if(number==10){
          cancel()
        }
      }

    }
    override def cancel(): Unit = {
      isRunning = false
    }
  }

