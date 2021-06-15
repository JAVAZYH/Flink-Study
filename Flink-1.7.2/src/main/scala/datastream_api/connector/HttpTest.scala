package datastream_api.connector

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * Created by zhoujiamu on 2019/1/9.
  */
object HttpTest {

  def getResponse(url: String, header: String = null): String = {
    val httpClient = HttpClients.createDefault()    // 创建 client 实例
    val get = new HttpGet(url)    // 创建 get 实例

//    if (header != null) {   // 设置 header
//      val json = JSON.parseObject(header)
//      json.keySet().toArray.map(_.toString).foreach(key => get.setHeader(key, json.getString(key)))
//    }

    val response = httpClient.execute(get)    // 发送请求
    val result: String = EntityUtils.toString(response.getEntity)
    result    // 获取返回结果
  }


  def postResponse(url: String, params: String = null, header: String = null): String ={
    val httpClient = HttpClients.createDefault()    // 创建 client 实例
    val post = new HttpPost(url)    // 创建 post 实例

    // 设置 header
//    if (header != null) {
//      val json = JSON.parseObject(header)
//      json.keySet().toArray.map(_.toString).foreach(key => post.setHeader(key, json.getString(key)))
//    }

    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }

    val response = httpClient.execute(post)    // 创建 client 实例
    EntityUtils.toString(response.getEntity, "UTF-8")   // 获取返回结果
  }


  def main(args: Array[String]): Unit = {

    val url =
      "http://allblue.oa.com/cgi-bin/tools/cgi_query_hbase_data_muti.py?result=html&index_id=0&flow_id=0&data_id=273935&data_date_begin=2020-08-28%2000:00:00&data_date_end=2020-08-31%2023:59:59&log_time_format=YYYY-MM-DD%20HH:MM:SS&save_time_long=7天&req_fields=19&key_0=3097554290&key_1=1&key_num=2&from_time=2020-08-28%2000:00:00&to_time=2020-08-31%2023:59:59&separate=,"

    println("This get response: ")
    println(getResponse(url))


//    val postUrl = "https://api.apiopen.top/searchAuthors"
//    val params = """{"company_list":["北京佛尔斯特金融信息服务有限公司"],"conditions":{}}"""
//
//    println("This post response: ")
//    println(postResponse(postUrl))
//    val header =
//      """
//        |{
//        |    "Accept": "application/json;charset=UTF-8",
//        |    "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept",
//        |    "Connection": "keep-alive",
//        |    "Content-Type": "application/json; charset=utf-8",
//        |    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI1TVAwU0w4TmVnciIsImV4cCI6MTU5NTU0NzE4MSwidWlkIjozMDR9.uf8VTB4yWh2nl2SmL8GJmDa6zewhMZF2QMh6SIcKFR4"
//        |}
//      """.stripMargin
//
//    // 需要token认证的情况，在header中加入Authorization
//    println(postResponse(postUrl, params, header))

  }

}
