package org.tjcj.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计英雄ban选次数
 */
public class BanMR1 {
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1、拿到一行数据
            String line = value.toString();
            if(!line.contains("agt")){//去除第一行数据
                //2、根据\t，进行切割
                String [] splits = line.split("\t");
                //3、写出到shuffle阶段
                for (int i=10;i<15;i++) {//找到ban选英雄
                    context.write(new Text(splits[i]),new LongWritable(1)); //[hadoop,1]
                }
            }
        }
    }
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for (LongWritable value : values) {
                count+=value.get();
            }
            context.write(key,new LongWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取当前hadoop开发环境
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop01:9000");
        //创建MapReduce任务
        Job job = Job.getInstance(configuration);
        //设置执行的主类
        job.setJarByClass(BanMR1.class);
        //设置Mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置Reducer
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输入路径
        FileInputFormat.addInputPath(job,new Path("/javaMkdir/lol.txt"));
        //设置输出路径，输出路径一定要确保不存在
        FileOutputFormat.setOutputPath(job,new Path("/mr/out/out6"));
        //提交任务
        System.out.println(job.waitForCompletion(true)?"success!!!":"failed!!!");
    }
}