package SecondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class SecondarySort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //TODO  修改代码的系统变量HADOOP_USER_NAME的内容为root
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "secondarySort");
        FileSystem fileSystem = FileSystem.get(URI.create(args[1]), configuration);

        //判断输入路径是否存在，存在就删除
        if (fileSystem.exists(new Path(args[1]))){
            fileSystem.delete(new Path(args[1]),true);
        }

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Person.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);
    }
}
