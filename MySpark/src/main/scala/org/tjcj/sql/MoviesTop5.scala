package org.tjcj.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, desc}

/**
 * 统计观影次数最多的10部电影
 */
object MoviesTop5 {
  def main(args: Array[String]): Unit = {
    //获取SparkSession对象
    val spark =SparkSession.builder().appName("movie").master("local[*]").getOrCreate()
    val df=spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\user.csv")
    //方式一：采用spark-sql提供df方法操作
    import spark.implicits._
    df.select($"movieName").groupBy($"movieName").count().orderBy(desc("count")).show(5)
    df.select($"movieName").groupBy($"movieName").count().orderBy($"count".desc).limit(10).repartition(1)
      .write.csv("E:\\Code\\dsjsx_fzx\\output\\out\\out5")


    //方式二：利用hive的sql方式
    df.createOrReplaceTempView("user")
    spark.sql("select movieName,count(*) as count from user group by movieName order by count desc").limit(10).repartition(1)
      .write.json("E:\\Code\\dsjsx_fzx\\output\\out\\out6")
  }
}