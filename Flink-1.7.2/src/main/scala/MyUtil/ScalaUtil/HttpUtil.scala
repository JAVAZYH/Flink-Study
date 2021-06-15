package MyUtil.ScalaUtil

import java.util

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/9/3
  * \* Time: 16:59
  * \*/
object HttpUtil {
  def main(args: Array[String]): Unit = {
    val dataId="277378"
    val startTime="2020-11-30 09:47:00"
    val endTime="2020-11-30 10:17:00"
    val uinName="open_id"
    val uinValue="2449513569"
    val gameId="4"

    val paramsMap = new mutable.HashMap[String,String]()
    paramsMap.put("act","update_data_chart")
    paramsMap.put("table_name",s"""allblue_es_index_oper_data_$dataId""")
    paramsMap.put("view_id",dataId)
    paramsMap.put("view_type","elasticsearch")
    paramsMap.put("row_conds","[]")
    paramsMap.put("col_conds","[]")
    paramsMap.put("factor_conds","[]")
    paramsMap.put("filter_conds",
      s"""[{\"field\":\"$uinName\",\"cond\":\"0\",\"value\":\"$uinValue\",\"comment\":\"\",\"col_alias\":\"$uinName\",\"is_custom_col\":0,\"type\":\"STRING\",\"is_vaild\":1},
         |{"field":"game_id","cond":"0","value":"$gameId","comment":"","col_alias":"game_id","is_custom_col":0,"type":"BIGINT","is_vaild":1}]
       """.stripMargin)
    paramsMap.put("text_conds","[{\"query_str\":\"\"}]")
    paramsMap.put("datetime_conds",s"""[\"@timestamp\",\"YYYY-MM-DD HH:MM:SS\",\"$startTime\",\"$endTime\"]""")
    paramsMap.put("page_conds","[1,10000]")
    paramsMap.put("order_conds","[]")
    paramsMap.put("login_user","jupitezhang")
    paramsMap.put("token","15856247035163WK714")
    val url="http://allblue.oa.com/cgi-bin/clmt/main.py"

    println(postRequest(url, paramsMap))


  }

  val LOG = LoggerFactory.getLogger(this.getClass)
  /**
    * 发送post请求
    * @param url 请求地址
    * @param params 请求参数，使用可变map
    * @return 字符串
    */
  def postRequest(url:String,params:mutable.Map[String, String] )={
    val httpClient = HttpClients.createDefault()
    val post=new HttpPost(url)
    val pairs = new util.ArrayList[NameValuePair]()
    val paramsMap: mutable.Map[String, String] =  params
    for (elem <- paramsMap) {
      pairs.add(new BasicNameValuePair(elem._1,elem._2))
    }
    post.setEntity(new UrlEncodedFormEntity(pairs))
    var result=""
    try{
    val response: CloseableHttpResponse = httpClient.execute(post)    // 发送请求
     result = EntityUtils.toString(response.getEntity)
    }catch {
      case exception: Exception=>
        result=""
        LOG.error("发送post请求失败，请求地址为："+url+"请求参数为"+params.mkString(","))
        println(exception.getMessage)
        LOG.error(exception.getMessage)
    }
    finally {
      httpClient.close()
    }
    result
  }

  /**
    *
    * @param url 请求地址
    * @return 字符串
    */
  def getRequest(url:String) ={
    val httpClient: CloseableHttpClient = HttpClients.createDefault()
    val get = new HttpGet(url)
    var result=""
    try{
      val response: CloseableHttpResponse = httpClient.execute(get)
       result = EntityUtils.toString(response.getEntity)
    }catch {
      case exception: Exception=>
        result=""
        LOG.error("发送get请求失败，请求地址为："+url)
        LOG.error(exception.getMessage)
    }
    finally {
      httpClient.close()
    }
    result
  }
}