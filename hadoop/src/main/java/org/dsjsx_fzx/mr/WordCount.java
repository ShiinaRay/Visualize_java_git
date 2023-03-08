package org.dsjsx_fzx.mr;

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
 *
 */
public class WordCount {
    /**
     * 执行当前类的主方法
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、获取当前Hadoop环境
        Configuration configuration = new Configuration();
        //2、设置提交的job
        Job job =Job.getInstance(configuration);
        //3、配置执行主类
        job.setJarByClass(WordCount.class);
        //4、配置Mapper类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //5、配置Reducer类
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //6、设置输入路径  默认输入类型--<LongWritable, Text>
        FileInputFormat.addInputPath(job,new Path("E:\\Code\\dsjsx_fzx\\mr\\input\\hadoop.txt"));
        //7、设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("E:\\Code\\dsjsx_fzx\\mr\\output\\out1"));
        //8、执行job
        System.out.println(job.waitForCompletion(true)?"success":"failed");;
    }

    /**
     * MapRecue核心类Mapper类
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable> {//根据分析类型指定完了  处理一个切片内容
        private LongWritable longWritable = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {//处理的一行数据
            //获取一行的数据
            String line =value.toString();
            //对数据进行变形处理
            String [] splits = line.split(",");
            for (String split : splits) {
                context.write(new Text(split),longWritable);//[hello,1]
            }
        }
    }
    /**
     shuffle:
     合并key---value [v1,v2,v3,v4]
     key.hashcode()%reduceNum -----0
     自然排序
     */

    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long nums = 0;
            for (LongWritable value : values) {
                nums+=value.get();//value---1
            }
            context.write(key,new LongWritable(nums));
        }
    }
}
