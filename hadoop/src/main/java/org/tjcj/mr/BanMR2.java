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
import java.util.TreeMap;

/**
 * 统计英雄ban选的排名
 */
public class BanMR2 {
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        private TreeMap<Long,String> treeMap = new TreeMap<>();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1、获取一行数据
            String line = value.toString();//c       5
            //2、切分字符串
            String [] splits = line.split("\t");
            treeMap.put(Long.parseLong(splits[1]),splits[0]);
        }

        /**
         * 整个MapReduce中只调用一次
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while(treeMap.size()>5){
                treeMap.remove(treeMap.firstKey());
            }
            for (Long aLong : treeMap.keySet()) {
                context.write(new Text(treeMap.get(aLong)),new LongWritable(aLong));
            }
        }
    }
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                context.write(key,value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取当前hadoop开发环境
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop01:9000");
        //创建MapReduce任务
        Job job = Job.getInstance(configuration);
        //设置执行的主类
        job.setJarByClass(BanMR2.class);
        //设置Mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置Reducer
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输入路径
        FileInputFormat.addInputPath(job,new Path("/mr/out/out6/part-r-00000"));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("/mr/out/out7" ));
        //提交任务
        System.out.println(job.waitForCompletion(true)?"success!!!":"failed!!!");
    }
}