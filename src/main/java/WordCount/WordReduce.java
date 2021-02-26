package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//输入参数和map的输出参数一致，输出参数由业务逻辑决定
public class WordReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    /*
    hello 1
    hello 1
    hello 1
    进行一个merge操作：
        hello 1，1，1
    key: hello
    value: List(1, 1, ...)
    处理的是所有相同key的value进行累加
*/
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable num:values){
            //map处理后的结果，经过combine不止有1。取出来每个v求和的一个操作
            //IntWritable变为int类型，需要调用get方法
            sum+=num.get();
        }
        // 输出最终结果
        context.write(key,new IntWritable(sum));
    }
}
