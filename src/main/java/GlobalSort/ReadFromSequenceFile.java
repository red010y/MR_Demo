package GlobalSort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class ReadFromSequenceFile {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        //args[0]指向SequenceFile文件位置
        SequenceFile.Reader.Option fileOption=SequenceFile.Reader.file(new Path(args[0]));
        SequenceFile.Reader reader=null;
        try {
            reader=new SequenceFile.Reader(conf,fileOption);
            //反射的创建kv
            Writable key= (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            Writable value= (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
            //获取当前位置
            long position=reader.getPosition();

            while (reader.next(key,value)){
                //同步点读*
                String synSeen=reader.syncSeen()?"*":"";
                System.out.printf("[%s%s]\t%s\t%s\n",position,synSeen,key,value);
                // beginning of next record
                //更新当前位置
                position=reader.getPosition();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(reader);
        }
    }
}

