package JudgeDataSkew;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WordReduce extends Reducer<Text, IntWritable, Text,IntWritable> {
    private int maxValueThreshold;//阈值

    //日志类
    private static final Logger logger = Logger.getLogger(WordReduce.class);

    //启动前就会调用setup
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        maxValueThreshold=10000;
    }
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        //用于记录键出现的次数
        int key_count=0;
        for (IntWritable count:values){
            sum+=count.get();
            key_count++;
        }
        //如果当前键超过10000个，则打印日志
        if (key_count>maxValueThreshold){
            logger.info("Received " + key_count + " values for key " + key);
        }
        // 输出最终结果
        context.write(key, new IntWritable(sum));
    }
}
