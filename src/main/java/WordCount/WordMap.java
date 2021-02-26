package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//Mapper泛型：输入kv的参数（偏移量，文本内容），输出的kv参数（单词，词频）业务逻辑决定
public class WordMap extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    //map方法针对输入分片的每个kv对,调用一次
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //对map输入进行处理，按空格进行切分
        String[] line = value.toString().split(" ");
        for (String word:line){
            // 每个单词出现１次，作为中间结果输出。
            // 把java对象变为hadoop可序列化的对象
            context.write(new Text(word),new IntWritable(1));
        }
    }

}
