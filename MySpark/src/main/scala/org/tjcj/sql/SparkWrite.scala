package org.tjcj.sql


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


object SparkWrite {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("write").appName("write").master("local[*]").getOrCreate()
    spark.sql("set spark.sql.adaptive.enabled=true")
    val rdd:RDD[Row]= spark.sparkContext.parallelize(List(
      Row(1,"张三","大四",18),
      Row(1,"李四","大四",19),
      Row(1,"王五","大四",20),
      Row(1,"赵六","大四",21),
      Row(1,"刘七","大四",22)
    ))
    val schema=StructType(List(
      StructField("id",DataTypes.IntegerType,false),
      StructField("name",DataTypes.StringType,false),
      StructField("grade",DataTypes.StringType,false),
      StructField("age",DataTypes.IntegerType,false)
    ))
    val df = spark.createDataFrame(rdd,schema)
    df.show()
    df.printSchema()
    df.repartition(1).write.json("E:\\Code\\dsjsx_fzx\\output\\out\\out3")
    import spark.implicits._
    val ds:Dataset[Student]= spark.createDataset(List(
      Student(1,"张三","大四",18),
      Student(1,"李四","大四",19),
      Student(1,"王五","大四",20),
      Student(1,"赵六","大四",21),
      Student(1,"刘七","大四",22)
    ))
    ds.show()
    ds.printSchema()
    ds.repartition(1).write.csv("E:\\Code\\dsjsx_fzx\\output\\out\\out4")

  }
}
case class Student(id:Int,name:String,grade:String,age:Int)