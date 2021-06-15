package MyUtil

import java.util.Properties

import Model.GlobalConfig


object KafkaUtil {
  val properties=new Properties()
  properties.setProperty("bootstrap.servers", GlobalConfig.BOOTSTRAP_SERVERS)
  properties.setProperty("group.id", GlobalConfig.GROUP_ID)

}
