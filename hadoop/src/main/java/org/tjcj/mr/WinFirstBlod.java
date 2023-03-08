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
 * 统计拿到1血队伍的胜率
 */
public class WinFirstBlod {
    public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1、拿到一行数据
            String line = value.toString();
            if(!line.contains("agt") && line.contains("Spring") && line.contains("LPL")){//根据要求进行数据的筛选
                //2、切分数据
                String [] splits = line.split("\t");
                int firstBlood = Integer.parseInt(splits[24]);
                if(firstBlood==1){//拿过1血队伍
                    context.write(new Text("FirstBlood"),new Text(splits[16]+","+firstBlood));
                }
            }
        }
    }
    public static class MyReducer extends Reducer<Text,Text,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum=0;//统计拿一血队伍总次数
            long win=0;//拿到1血且胜利情况
            for (Text value : values) {
                String line =value.toString();
                String [] str = line.split(",");
                int result=Integer.parseInt(str[0]);
                if(result==1){//队伍获胜了
                    win++;
                }
                sum++;
            }
            context.write(new Text("拿1血队伍的胜率"),new LongWritable(win*100/sum));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取当前hadoop开发环境
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop01:9000");
        //创建MapReduce任务
        Job job = Job.getInstance(configuration);
        //设置执行的主类
        job.setJarByClass(WinFirstBlod.class);
        //设置Mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置Reducer
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输入路径
        FileInputFormat.addInputPath(job,new Path("/javaMkdir/lol.txt"));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("/mr/out/out9"));
        //提交任务
        System.out.println(job.waitForCompletion(true)?"success!!!":"failed!!!");
    }
}