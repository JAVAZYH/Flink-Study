package MyUtil.ScalaUtil

import java.util

import com.google.gson.{Gson, JsonArray, JsonObject}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Author atguigu
  * Date 2020/6/24 9:22 */
object HBaseUtil {
  // 1. 先获取到hbase的连接
  val conf=HBaseConfiguration.create()
 conf.set("hbase.zookeeper.quorum","9.134.217.5")
  conf.set("hbase.zookeeper.property.clientPort","2181")

//  conf.set("hbase.zookeeper.quorum","ps-hbase-zk-1.tencent-distribute.com,ps-hbase-zk-2.tencent-distribute.com,ps-hbase-zk-3.tencent-distribute.com,ps-hbase-zk-4.tencent-distribute.com,ps-hbase-zk-5.tencent-distribute.com")
//  conf.set("hbase.zookeeper.property.clientPort","2181")
//  conf.set("zookeeper.znode.parent","/hbase_hy_security")

  val conn: Connection = ConnectionFactory.createConnection(conf)

  def main(args: Array[String]): Unit = {
//    println(getTableColumns("tsy_ares_test"))
//    createTable("ares_tmp","c")
//    putData("ares_tmp","111","c","uin",1531351300,"153428")
//    truncateTable("tsy_ares_test")
//    deleteTable("ares_tmp")
//    println(getRandomData("tsy_ares_test"))
//    println(getDataByRowKey("tsy_ares_test", "1534280189"))
    val gson = new Gson()
    println(gson.toJson(getAllTsyTables()))

  }

  def scanData(tableName: String) = {
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
//    val filter =
//      new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOperator.EQUAL, Bytes.toBytes("abc"))
//    filter.setFilterIfMissing(true)
//    scan.setFilter(filter)

    val results: ResultScanner = table.getScanner(scan)
    // 从scanner拿到所有数据
    for (result <- results) {
      val cells: util.List[Cell] = result.listCells() // rawCells
      if (cells != null) {
        for (cell <- cells) {
          println(
            s"""
               |row = ${Bytes.toString(CellUtil.cloneRow(cell))}
               |cf = ${Bytes.toString(CellUtil.cloneFamily(cell))}
               |name = ${Bytes.toString(CellUtil.cloneQualifier(cell))}
               |value = ${Bytes.toString(CellUtil.cloneValue(cell))}
               |----------------
               |""".stripMargin)
        }
      }

    }


    table.close()


  }

  /**
    * 根据rowkey模糊查询出所有数据
    * @param tableName
    * @param rowKey
    * @return 返回有序的两个结果集
    */
  def getDataByRowKey(tableName: String, rowKey: String) = {
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
    val rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(rowKey))
    scan.setFilter(rowFilter)
    val results: ResultScanner = table.getScanner(scan)
    //[123,[("lol_game_iuin","123"),("lol_game_igameseq","222"),]]
    //构造两个有序的java treemap集合存储数据
    var resultMap = new util.TreeMap[String, util.TreeMap[String, String]]()

    // 这个是用来在java的集合和scala的集合之间互转  (隐式转换)

    for (result <- results) {
      val cells: util.List[Cell] = result.listCells() // rawCells
      if (cells != null) {
        for (cell <- cells) {
          val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
          val cf = Bytes.toString(CellUtil.cloneFamily(cell))
          val columnName = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          val columnValueMap = resultMap.getOrDefault(rowKey, new util.TreeMap[String, String]())
          columnValueMap.put(columnName,value)
          resultMap.put(rowKey, columnValueMap)
        }
      }
    }
    table.close()
//    import scala.collection.JavaConverters._
    resultMap
  }


  /**
    * 根据传入的参数使用get从hbase中查询数据
    * @param tableName
    * @param rowKey
    * @param cf
    * @param columnName
    * @return
    */
  def getData(tableName: String, rowKey: String, cf: String, columnName: String="") = {
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    get.addFamily(Bytes.toBytes(cf))
    val result: Result = table.get(get)

    // 这个是用来在java的集合和scala的集合之间互转  (隐式转换)

    val cells: util.List[Cell] = result.listCells()
    var resultStr=""
    if (cells != null) {
      for (cell <- cells) {
        resultStr+=s"""
                     |row = ${Bytes.toString(CellUtil.cloneRow(cell))}
                     |cf = ${Bytes.toString(CellUtil.cloneFamily(cell))}
                     |name = ${Bytes.toString(CellUtil.cloneQualifier(cell))}
                     |value = ${Bytes.toString(CellUtil.cloneValue(cell))}
                     |----------------
                     |""".stripMargin
      }
    }
    table.close()
    resultStr

  }

  /**
   * 根据rowkey查询某个列族下符合某个列名的前缀的数据
   * @param tableName
   * @param rowKey
   * @param cf
   * @param columnName
   * @return 返回字符串按照|进行分割
   */
  def getDataWithColumn(tableName: String, rowKey: String, cf: String, columnName: String="") = {
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    //过滤出列名的前缀为student的数据
    val filter = new ColumnPrefixFilter(Bytes.toBytes(columnName))
    get.setFilter(filter)
    get.addFamily(Bytes.toBytes(cf))
    val result: Result = table.get(get)

    // 这个是用来在java的集合和scala的集合之间互转  (隐式转换)

    val cells: util.List[Cell] = result.listCells() // rawCells
    var resultStr=""
    if (cells != null) {
      for (cell <- cells) {
        resultStr+=s"""${Bytes.toString(CellUtil.cloneRow(cell))}|
                      |${Bytes.toString(CellUtil.cloneFamily(cell))}|
                      |${Bytes.toString(CellUtil.cloneQualifier(cell))}|
                      |${Bytes.toString(CellUtil.cloneValue(cell))}""".stripMargin
      }
    }

    table.close()
    resultStr

  }


  def deleteData(tableName: String, rowKey: String, cf: String, columnName: String) = {
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val delete = new Delete(Bytes.toBytes(rowKey))
    //        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName))
    delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(columnName)) // 删除所有版本
    table.delete(delete)

    table.close()

  }

  def putData(tableName: String, rowKey: String, cf: String, columnName: String, timestamp:Long,value: String) = {

    // 1. 先获取到表对象,客户端到表的连接
    val table: Table = conn.getTable(TableName.valueOf(tableName))

    // 2. 调用表对象的put
    // 2.1 把需要添加的数据封装到一个Put对象   put '', rowKey, '', ''
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName), timestamp,Bytes.toBytes(value))
    // 2.2 提交Put对象
    table.put(put)
    // 3. 关闭到table的连接
    table.close()
  }

  def getRandomData(tableName: String) ={
    val admin: Admin = conn.getAdmin
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
    scan.setCaching(3)
    val results: ResultScanner = table.getScanner(scan)
    val resultArr = new ArrayBuffer[String]()

    // 从scanner拿到所有数据
    for (result <- results) {
      val cells: util.List[Cell] = result.listCells() // rawCells
      if (cells != null) {
        for (cell <- cells) {
            resultArr.append(
            s"""
               |row = ${Bytes.toString(CellUtil.cloneRow(cell))}
               |cf = ${Bytes.toString(CellUtil.cloneFamily(cell))}
               |name = ${Bytes.toString(CellUtil.cloneQualifier(cell))}
               |value = ${Bytes.toString(CellUtil.cloneValue(cell))}
               |----------------
               |""".stripMargin)
        }
      }
    }

    admin.close()
    table.close()
    resultArr.mkString(",")
  }

  /**
    * 创建命名空间
    * @param name
    */
  def createNS(name: String) = {
    val admin: Admin = conn.getAdmin
    if (!nsExists(name)) {
      val nd: NamespaceDescriptor.Builder = NamespaceDescriptor.create(name)
      admin.createNamespace(nd.build())
    } else {
      println(s"你要创建的命名空间: ${name}已经存在")
    }
    admin.close()
  }

  /**
    * 判断命名空间是否存在
    * @param name
    * @return
    */
  def nsExists(name: String): Boolean = {
    val admin: Admin = conn.getAdmin
    val nss: Array[NamespaceDescriptor] = admin.listNamespaceDescriptors()
    val r = nss.map(_.getName).contains(name)
    admin.close()
    r
  }

  /**
    * 删除HBase表
    * @param tableName
    * @return
    */
  def deleteTable(tableName: String): Boolean = {
    val admin: Admin = conn.getAdmin
    var result=false
    if (tableExists(tableName)) {
      admin.disableTable(TableName.valueOf(tableName))
      admin.deleteTable(TableName.valueOf(tableName))
      result=true
    }
    admin.close()
    result
  }

  /**
    * 清空hbase表内容，并且保留rowkey预分区
    * @param tableName
    * @return
    */
  def truncateTable(tableName:String): Boolean ={
    val admin: Admin = conn.getAdmin
    var result=false
    if(tableExists(tableName)){
      admin.disableTable(TableName.valueOf(tableName))
      admin.truncateTable(TableName.valueOf(tableName),true)
      result=true
    }
    admin.close()
    result
  }


  /**
    * 获取该表的所有列名,注意：不一定是最全的那条数据的列名
    * @param tableName
    * @return
    */
  def getTableColumns(tableName:String)={

    val admin: Admin = conn.getAdmin
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    var rowKey=""
    val columnNameArr=new ArrayBuffer[String]()
    val scan = new Scan()
    scan.setBatch(1)
    scan.setCaching(1)
    val results: ResultScanner = table.getScanner(scan)
    if(results.nonEmpty){
    // 从scanner拿到所有数据
    for (result <- results) {
      val cells: util.List[Cell] = result.listCells() // rawCells
      if (cells != null) {
        rowKey=Bytes.toString(CellUtil.cloneRow(cells.head))
      }
    }


      val get = new Get(Bytes.toBytes(rowKey))
      get.addFamily(Bytes.toBytes("c"))
      val tableResult: Result = table.get(get)
      val cells: util.List[Cell] = tableResult.listCells() // rawCells
      if (cells != null) {
        for (cell <- cells) {
          val columnName = Bytes.toString(CellUtil.cloneQualifier(cell))
          columnNameArr.append(columnName)
        }
      }
    }
    table.close()
    admin.close()
    columnNameArr.mkString(",")
  }
  /*
    * 创建指定的表
    * 指定表名和列族即可
    *
    * @param name
   */
  def createTable(name: String, cf: String,maxVersions:Int=3,expireTime:Int=0): Boolean = {
    val admin: Admin = conn.getAdmin
    val tableName = TableName.valueOf(name)

    if (tableExists(name)) return false

    val descriptor = new HTableDescriptor(tableName)
    //    descriptor.setConfiguration("hbase.table.sanity.checks","false")
    val hColumnDescriptor: HColumnDescriptor = new HColumnDescriptor(cf)
    //设置列族的属性
    //保存的最大版本数
    hColumnDescriptor.setMaxVersions(maxVersions)
    //数据的过期时间,单位是秒，如果为0说明没设置过期时间，默认保存永久
    if(expireTime!=0){
      hColumnDescriptor.setTimeToLive(expireTime)
    }
    //设置数据压缩方式
    hColumnDescriptor.setCompressionType(Algorithm.GZ)
    //设置数据保存再内存中/
    hColumnDescriptor.setInMemory(true)
    //开启块缓存
    hColumnDescriptor.setBlockCacheEnabled(true)
    //开启布隆过滤器为row级别
    hColumnDescriptor.setBloomFilterType(BloomType.ROW)
    //设置内存中数据压缩格式，后续在设置
    descriptor.addFamily(hColumnDescriptor)


    //        admin.createTable(td.build())
    //指定rowkey的分区情况,默认是3个0到3个9
    val split_arr: ArrayBuffer[Array[Byte]] = ArrayBuffer[Array[Byte]]()
    for(ele <- 0 to 9){
      split_arr.append(Bytes.toBytes(ele.toString+ele.toString+ele.toString))
    }

    //    val splites = Array(Bytes.toBytes("111"), Bytes.toBytes("222"), Bytes.toBytes("333"))
    //    admin.createTable(td.build())
    admin.createTable(descriptor,split_arr.toArray)
    admin.close()
    true
  }

  /*
  *
   * 判断表是否存在
   *
   * @param name
   * @return
  */
  def tableExists(name: String): Boolean = {

    // 2. 获取管理对象 Admin
    val admin: Admin = conn.getAdmin
    // 3. 利用Admin进行各种操作
    val tableName: TableName = TableName.valueOf(name)
    val b = admin.tableExists(tableName)
    // 4. 关闭Admin
    admin.close()
    b
  }


  def getAllTsyTables() ={
    // 2. 获取管理对象 Admin
    val admin: Admin = conn.getAdmin
    val names: Array[TableName] = admin.listTableNames(".*tsy.*")
    val jsonArray = new JsonArray()
    for (name <- names) {
      val tableName=name.getNameAsString
        val jsonObj = new JsonObject()
        jsonObj.addProperty("tableName",tableName)
        jsonObj.addProperty("columns", HBaseUtil.getTableColumns(name.getNameAsString))
        jsonArray.add(jsonObj)
    }
    admin.close()
    jsonArray
  }

  def closeConnection() = conn.close()
}

