package org.tjcj.sql

import org.apache.spark.sql.SparkSession

/**
 *
 */
object MyFirstSparkSql {
  def main(args: Array[String]): Unit = {
    //创建SparkSession对象
    val spark = SparkSession.builder().appName("myFirstSparkSession").master("local[*]").getOrCreate()
    val df = spark.read.json("E:\\Code\\dsjsx_fzx\\input\\people.json")
    df.show()
    //打印二维表的格式
    df.printSchema()
    //类似于sql语句格式进行查询数据
    df.select("name","age").show()
    //引入scala，隐式转换
    import spark.implicits._
    //让字段进行运算
    df.select($"name",$"age"+1).show()
    //加上条件
    df.select("name").where("age>20").show()
    //进行分组统计
    df.groupBy("age").count().show()
    //创建一个临时表
    df.createOrReplaceTempView("people")
    spark.sql("select age,count(*) as count1 from people group by age").show()
    //关闭spark
    spark.stop()
  }
}