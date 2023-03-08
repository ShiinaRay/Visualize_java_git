package org.tjcj.sql

import org.apache.spark.sql.SparkSession

/**
 * 统计电影的文件情况
 */
object GradeTop10 {
  def main(args: Array[String]): Unit = {
    //获取SparkSession对象
    val spark =SparkSession.builder().appName("movie").master("local[*]").getOrCreate()
    val df=spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\movie.csv").distinct()
    //采用sql方式实现
    //创建临时表
    df.createOrReplaceTempView("movie")
    spark.sql("select distinct(movieName),grade from movie where grade >8 order by grade desc").limit(10)
      .write.json("E:\\Code\\dsjsx_fzx\\output\\out\\out8")
    //关闭spark session对象
    spark.stop()
  }
}