package SecondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMap extends Mapper<LongWritable, Text, Person, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String name = line[0];
        int age = Integer.parseInt(line[1]);
        int salary = Integer.parseInt(line[2]);
        Person person = new Person(name, age, salary);
        context.write(person,NullWritable.get());
    }
}
