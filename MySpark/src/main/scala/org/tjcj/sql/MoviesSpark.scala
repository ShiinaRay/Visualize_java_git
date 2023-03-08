package org.tjcj.sql

import org.apache.spark.sql.SparkSession

/**
 * 统计电影的文件情况
 */
object MoviesSpark {
  def main(args: Array[String]): Unit = {
    //获取SparkSession对象
    val spark =SparkSession.builder().appName("movie").master("local[*]").getOrCreate()
    val df1=spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\movie.csv")
    df1.show(10)
    df1.printSchema()
    val df2=spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\user.csv")
    df2.show(10)
    df2.printSchema()
  }
}