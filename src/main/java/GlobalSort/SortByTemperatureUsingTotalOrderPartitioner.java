package GlobalSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.net.URI;

//一个MapReduce程序，用于使用TotalOrderPartitioner使用IntWritable键对SequenceFile进行排序，以对数据进行全局排序
public class SortByTemperatureUsingTotalOrderPartitioner{

    //两个参数：/ncdc/sfoutput /ncdc/totalorder
    public static void main(String[] args) throws Exception {
        //TODO  修改代码的系统变量HADOOP_USER_NAME的内容为root
        System.setProperty("HADOOP_USER_NAME","root");

        if (args.length != 2) {
            System.out.println("<input> <output>");
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, SortByTemperatureUsingTotalOrderPartitioner.class.getSimpleName());
        job.setJarByClass(SortByTemperatureUsingTotalOrderPartitioner.class);

        //上面代码输出的sequencefile路径作为输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //sequenceFile作为输入类型
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        //设置sequenceFile作为输出格式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setCompressOutput(job, true);
        //SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

        job.setNumReduceTasks(3);

        //分区器，需要分区文件
        job.setPartitionerClass(TotalOrderPartitioner.class);

        //进行采样，生成分区文件
        //第一个参数：采样率（从分区中取百分之多少）；第二个参数：最大样本数（样本最多10000个）；第三个参数：最大分区数（最多对10个分区取样）；三者任一满足，就停止采样
        InputSampler.Sampler<IntWritable, Text> sampler =
                new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
        //将采样结果放入一个文件中
        //根据采样结果推测整个文件的数据分布情况，进行合理的分区
        InputSampler.writePartitionFile(job, sampler);

        //TotalOrderPartitioner获取采样结果也就是刚刚写入的分区文件
        // Add to DistributedCache
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        URI partitionUri = new URI(partitionFile);
        //将分区文件添加到分布式缓存中（如果不进行缓存，每个节点上要进行map task就要从文件所在服务器上获取，网络通信效率低。进行分布式缓存后可以直接从本地获取这个文件，效率高）
        job.addCacheFile(partitionUri);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
