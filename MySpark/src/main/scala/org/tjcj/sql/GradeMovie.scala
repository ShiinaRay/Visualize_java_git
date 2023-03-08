package org.tjcj.sql

import org.apache.spark.sql.SparkSession

/**
 * 统计电影的文件情况
 */
object GradeMovie {
  def main(args: Array[String]): Unit = {
    //获取SparkSession对象
    val spark =SparkSession.builder().appName("movie").master("local[*]").getOrCreate()
    val df1=spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\movie.csv").distinct()
    val df2=spark.read.option("header",true).csv("E:\\Code\\dsjsx_fzx\\input\\user.csv").distinct()
    //创建临时表
    df1.createOrReplaceTempView("movie")
    df2.createOrReplaceTempView("user")
    spark.sql("select m.movieName,m.grade,count(*) as count from movie m left join user u on m.movieName = u.movieName " +
      "where m.grade >9.0 and substr(u.time,0,7) = '2018-01' group by m.movieName,m.grade order by count desc").limit(10)
      .repartition(1)
      .write.json("E:\\Code\\dsjsx_fzx\\output\\out\\out14")
  }
}