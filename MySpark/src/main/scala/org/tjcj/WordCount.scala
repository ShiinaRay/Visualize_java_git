package org.tjcj

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark中经典案例
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1、获取SparkContext对象
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2、WordCount核心代码
    sc.textFile("E:\\Code\\dsjsx_fzx\\mr\\input\\a.txt").flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).
      sortBy(_._2,false).collect().foreach(println)
  }
}