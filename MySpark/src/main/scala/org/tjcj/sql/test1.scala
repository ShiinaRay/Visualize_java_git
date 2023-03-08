package org.tjcj.sql

import org.apache.spark.sql.SparkSession

/**
 * 各个国家数据量
 */
object test1 {
  def main(args: Array[String]): Unit = {
    //获取SparkSession对象
    val spark =SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val df=spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\test.csv")
    df.createOrReplaceTempView("test")
    spark.sql("select address,count(*) as count from test group by address order by count desc").limit(10)
      .write.json("E:\\Code\\dsjsx_fzx\\output\\out\\out15")
    spark.stop()

  }
}