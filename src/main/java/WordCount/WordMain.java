package WordCount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //TODO  修改代码的系统变量HADOOP_USER_NAME的内容为root
        System.setProperty("HADOOP_USER_NAME","root");

        //判断是否有输入输出参数
        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }

        Configuration conf = new Configuration();

        //集群模式指定jar包位置--参数：jar包位置的配置参数，jar包位置
//        conf.set("mapreduce.job.jar","H:\\code_DT\\MR_Demo\\target\\MR_Demo-1.0-SNAPSHOT.jar");

        //job就是整个代码的逻辑，规定map，reduce分别做什么
        //获取job实例--参数：conf，job名称（取当前类的路径）
        Job job = Job.getInstance(conf, "wc");

        // 打成jar包后通过这行代码找到程序的入口
        job.setJarByClass(WordMain.class);

        // 通过job设置输入/输出格式
        // MR的默认输入格式是TextInputFormat，按照行读。所以下两行可以注释掉
        //还可以设置按照一定的单词数读，读数据库DBImputFormat，读N行NLineInputFormat
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入/输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 设置处理Map/Reduce阶段的类
        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);

        //设置map端combiner。用的也是reduce的代码
        job.setCombinerClass(WordReduce.class);

        //如果map、reduce的输出的kv对类型一致，直接设置reduce的输出的kv对就行；
        //如果不一样，需要分别设置map, reduce的输出的kv类型
        //job.setMapOutputKeyClass(.class)
        //job.setMapOutputValueClass(IntWritable.class);

        // 设置最终reduce输出key/value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置reduce task个数
        job.setNumReduceTasks(4);

        //自定义分区器
        job.setPartitionerClass(CustomPartitioner.class);

        // 提交作业
        job.waitForCompletion(true);
    }
}
