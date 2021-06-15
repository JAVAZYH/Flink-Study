package datastream_api.asyncIO

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
object AsyncReadTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.enableCheckpointing(60*1000L)//1分钟一次checkpoint
    //    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    //    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//精准一次
    //    checkpointConfig.setMinPauseBetweenCheckpoints(30*1000L)//checkpoint的间隔时间为30s
    //    checkpointConfig.setCheckpointTimeout(10*1000L)//设置checkpoint超时时间，默认10分钟
    //    //任务cancel时保留checkpoint
    //    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置状态后端为rocksdb
    //    val stateBackend = new RocksDBStateBackend("")
    //    env.setStateBackend(stateBackend)

    //设置重启策略，重启3次间隔10
    //
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(10)))

    val input_ds: DataStream[String] = env.addSource(new MySource.TeacherSource)
    //使用异步io写入数据到HBase中

    /**
     *输入流
     * 异步函数
     * 超时等待时间
     * 时间单位
     * 容量=允许的并发数,超过10个请求就会反压之前的节点
     */
    val reslut: DataStream[String] = AsyncDataStream
      .orderedWait(input_ds,new MyHBaseReadAsyncFunction,101,TimeUnit.SECONDS,10)


//    input_ds.print()
    reslut.print()
    env.execute()


  }

}
