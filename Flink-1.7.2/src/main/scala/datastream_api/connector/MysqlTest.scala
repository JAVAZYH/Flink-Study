package datastream_api.connector

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._



object MysqlTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input_ds: DataStream[String] = env.addSource(new MySource.StudentSource)

    def getDataFromMysql()={
      val URL = "jdbc:mysql://localhost:3306/db_test"
      val USERNAME = "root"
      val PASSWORD = "root"

      var connection: Connection = null
      var statement: PreparedStatement = null
      var resultSet: ResultSet = null

      try {
        // 1.加载Driver
        classOf[com.mysql.jdbc.Driver]
        // 2.建立连接
        connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)
        // 3.预加载语句
        statement = connection.prepareStatement("SELECT * FROM tbl_dept WHERE id = ?")
        statement.setInt(1, 1) // 设置值
        // 4.执行
        resultSet = statement.executeQuery()
        // 取值
        while (resultSet.next()){
          val id = resultSet.getInt("id")
          val deptName = resultSet.getString("deptName")
          println(s"id: $id, name: $deptName")
        }
      } catch {
        case e:Exception => e.printStackTrace()
      } finally {
        resultSet.close()
        statement.close()
        connection.close()
      }

    }

  }




}
