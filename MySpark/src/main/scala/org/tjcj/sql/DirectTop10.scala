package org.tjcj.sql

import org.apache.spark.sql.SparkSession

/**
 * 近年来那些导演拍的电影较多
 */
object DirectTop10 {
  def main(args: Array[String]): Unit = {
    //1、获取SparkSession对象
    val spark = SparkSession.builder().appName("directTop10").master("local[*]").getOrCreate()
    //2、获取要处理的文件
    val df = spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\movie.csv").distinct()
    //方式一：采用spark-sql中方言方式
    import spark.implicits._
    df.repartition(1).select("direct").groupBy("direct").count().orderBy($"count".desc).limit(10)
      .write.json("D:\\spark-sql\\out\\out11")
    //方式二：采用hive-sql的方式
    df.createOrReplaceTempView("movie")
    spark.sql("select direct,count(*) as count from movie group by direct order by count desc").limit(10).repartition(1)
      .write.json("E:\\Code\\dsjsx_fzx\\output\\out\\out12")
  }
}