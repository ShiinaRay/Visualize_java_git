package org.tjcj.sql

import org.apache.spark.sql.SparkSession

/**
 * 各个国家数据量
 */
object CountTop10 {
  def main(args: Array[String]): Unit = {
    //获取SparkSession对象
    val spark =SparkSession.builder().appName("movie").master("local[*]").getOrCreate()
    val df=spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\movie.csv")
    df.createOrReplaceTempView("movie")
    spark.sql("select address,count(*) as count from movie group by address order by count desc").limit(10)
      .write.json("E:\\Code\\dsjsx_fzx\\output\\out\\out10")
    spark.stop()

  }
}