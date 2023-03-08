package org.tjcj

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计英雄联盟中ban选英雄排名前五的英雄
 */
object BanTop5 {
  def main(args: Array[String]): Unit = {
    //1、获取SparkContext对象
    val conf:SparkConf = new SparkConf().setAppName("ban5").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)
    //2、获取一个RDD对象,通过外部文件方式获取RDD
    val rdd:RDD[String] = sc.textFile("hdfs://hadoop01:9000/javaMkdir/lol.txt")
    //3、过滤文件，找到需要处理的信息
    val filterRDD:RDD[String] = rdd.filter(!_.contains("agt")).filter(_.contains("Spring"))
    //4、对原始数据进行变形
    val mapRDD = filterRDD.map(x=>{//x表示传入一行数据
      var splits=x.split("\t")
      var str=""
      var i=10
      while(i<15){
        str+=splits(i)+","
        i+=1
      }
      str.substring(0,str.length-1) //新的字符串
    })
    //5、亚平处理
    val flatMapRDD = mapRDD.flatMap(_.split(","))
    //6、变形
    val mapRDD1 = flatMapRDD.map((_,1))
    //7、统计
    val reduceRDD = mapRDD1.reduceByKey(_+_)
    //8、排序
    val sortRDD =  reduceRDD.sortBy(_._2,false)
    //val修饰常量，var修饰变量
    //9、取值
    val array = sortRDD.take(5)
    //10、转换保存
    sc.makeRDD(array).repartition(1).saveAsTextFile("hdfs://hadoop01:9000/spark/out/out2")
  }
}