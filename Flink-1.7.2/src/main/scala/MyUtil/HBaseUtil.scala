package com.atguigu.hbase

import java.util

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{BinaryComparator, ColumnPrefixFilter, CompareFilter, QualifierFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}

/**
  * Author atguigu
  * Date 2020/6/24 9:22 */
object HBaseUtil {
  // 1. 先获取到hbase的连接
  val conf=HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.quorum","hadoop120,hadoop121,hadoop122")
  conf.set("hbase.zookeeper.quorum","9.134.217.5")
  conf.set("hbase.zookeeper.property.clientPort","2181")
  val conn: Connection = ConnectionFactory.createConnection(conf)

  def main(args: Array[String]): Unit = {

    //        putData("user", "1001", "info", "name", "lisi")
    //        putData("user", "1002", "info", "name", "ww")

    //        deleteData("user1", "1001", "info", "age")

    //        getData("user", "1001", "info", "name")
//    scanData("lol_game_all")
//    deleteTable("tsy_test")
//    createTable("tsy_test","c")
    //    closeConnection()
        println(tableExists("test"))

//    putData("student","1001","c","id",9999,"1111")

//    println(getData("tsy_test", "111", "c"))
//    println(getDataWithColumn("tsy_test", "222", "c","student"))


  }

  def scanData(tableName: String) = {
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
//    val filter =
//      new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOperator.EQUAL, Bytes.toBytes("abc"))
//    filter.setFilterIfMissing(true)
//    scan.setFilter(filter)

    val results: ResultScanner = table.getScanner(scan)

    import scala.collection.JavaConversions._
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
    //过滤出列名的前缀为student的数据
    val filter = new ColumnPrefixFilter(Bytes.toBytes("school"))
    //查询出以student开头的
//    val filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("student")))
    get.setFilter(filter)
    get.addFamily(Bytes.toBytes(cf))
//    get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName))
    val result: Result = table.get(get)

    // 这个是用来在java的集合和scala的集合之间互转  (隐式转换)
    import scala.collection.JavaConversions._

    val cells: util.List[Cell] = result.listCells() // rawCells
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
    import scala.collection.JavaConversions._

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
    //        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName + "abc"), Bytes.toBytes(value + "efg"))
    // 2.2 提交Put对象
    table.put(put)

    // 3. 关闭到table的连接
    table.close()

  }

//创建命名空间
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

  //判断命名空间是否存在
  def nsExists(name: String): Boolean = {
    val admin: Admin = conn.getAdmin
    val nss: Array[NamespaceDescriptor] = admin.listNamespaceDescriptors()
    val r = nss.map(_.getName).contains(name)
    admin.close()
    r
  }

//删除表
  def deleteTable(name: String) = {
    val admin: Admin = conn.getAdmin
    if (tableExists(name)) {
      admin.disableTable(TableName.valueOf(name))
      admin.deleteTable(TableName.valueOf(name))
    }

    admin.close()
  }

  /*
   * 创建指定的表
   * 指定表名和列族即可
   *
   * @param name
  */
  def createTable(name: String, cfs: String*): Boolean = {
    val admin: Admin = conn.getAdmin
    val tableName = TableName.valueOf(name)

    if (tableExists(name)) return false

    val descriptor = new HTableDescriptor(tableName)

    cfs.foreach(cf => {
      descriptor.addFamily(new HColumnDescriptor(cf))
    })

    //        admin.createTable(td.build())
    //指定rowkey的分区情况
    val splites = Array(Bytes.toBytes("111"), Bytes.toBytes("222"), Bytes.toBytes("333"))
//    admin.createTable(td.build())
    admin.createTable(descriptor,splites)
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


  def closeConnection() = conn.close()
}

