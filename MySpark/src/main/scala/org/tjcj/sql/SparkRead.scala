package org.tjcj.sql

import org.apache.spark.sql.SparkSession

/**
 * 读取不同类型的文件
 */
object SparkRead {
  def main(args: Array[String]): Unit = {
    //构建SparkSession对象
    val spark = SparkSession.builder().appName("sparkRead").master("local[*]").getOrCreate()
    //读取json文件
    val df = spark.read.json("E:\\Code\\dsjsx_fzx\\input\\employees.json")
    df.show()
    df.printSchema()
    //读取csv文件
    val df1= spark.read.csv("E:\\Code\\dsjsx_fzx\\input\\lol.csv")
    df1.show()
    df1.printSchema()
    //读取普通文本文件
    val df2=spark.read.text("E:\\Code\\dsjsx_fzx\\input\\student.txt")
    df2.show()
    df2.printSchema()
    //读取orc

    //读取
    val df4 =spark.read.parquet("E:\\Code\\dsjsx_fzx\\input\\users.parquet")
    df4.show()
    df4.printSchema()
  }
}