package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
//继承的Partition的泛型，是map阶段输出的kv
public class CustomPartitioner extends Partitioner<Text, IntWritable> {
    //设置分区的对应关系
    static HashMap<String, Integer> dict = new HashMap<>();
    static {
        //0号分区，放Dear
        dict.put("Dear",0);
        dict.put("Bear", 1);
        dict.put("River", 2);
        dict.put("Car", 3);
    }

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        //根据text的内容去指定对应分区
        return dict.get(text.toString());
    }
}
