package GlobalSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


// 一个MapReduce程序，用于将天气数据转换为SequenceFile格式
public class SortDataPreprocessor {

    static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        //这个有什么用？这是自己创建的一个对象，气象局数据的实体，用于解析气象局的数据
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //文本中每行的数据：0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999
            //解析，给内部的成员变量赋值
            parser.parse(value);
            //这里进行一个数据清洗，有效的气象才写出
            if (parser.isValidTemperature()) {
                //输出的key是气温，value是具体的记录，根据气温进行排序。
                context.write(new IntWritable(parser.getAirTemperature()), value);
            }
        }
    }

    //指定文件夹，读取文件夹下所有文件1901和1902
    public static void main(String[] args) throws Exception {
        //TODO  修改代码的系统变量HADOOP_USER_NAME的内容为root
        System.setProperty("HADOOP_USER_NAME","root");

        if (args.length != 2) {
            System.out.println("<input> <output>");
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "SortDataPreprocessor");
        job.setJarByClass(SortDataPreprocessor.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CleanerMapper.class);
        //最终map输出的键、值类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        //当不指定Reduce时候，系统会使用缺省的reduce函数
        //不想要reduce就必须设置job.setNumReduceTasks(0);reduce个数为0
        //如果设置reducer任务数为0，map端不会执行combiner，sort，merge操作
        //会直接输出无序结果（读一行，输一行）
        //并且输出的文件数量，与读取的文件数相同
        job.setNumReduceTasks(0);

        //MR的wordcount用的是TextInputFormat和TextOutputFormat默认就是所以不用设置
        //这里以sequencefile的格式作为输出，必须设置
        //这里设置了sequenceFile输出，查看文件的时候看到的是字节码
        // 5345 5106 206f 7267 2e61 7061 6368 652e
        // 必须用sequenceFile的方法读取ReadFromSequenceFile（结果无序）
        //[81791]	-72	0029227070999991902033106004+62167+030650FM-12+010299999V0203601N002119999999N0000001N9-00721+99999099821ADDGF106991999999999999999999
        //[81791]	-17	0029227070999991902033113004+62167+030650FM-12+010299999V0200201N004119999999N0000001N9-00171+99999099641ADDGF108991999999999999999999
        //[81791]	-33	0029227070999991902033120004+62167+030650FM-12+010299999V0200501N005119999999N0000001N9-00331+99999099661ADDGF108991999999999999999999
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //设置sequencefile的压缩
        SequenceFileOutputFormat.setCompressOutput(job, true);

        //设置sequencefile的压缩算法
        //SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        //设置sequencefile的文件压缩格式block
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
