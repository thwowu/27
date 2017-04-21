import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Exercisemapper extends Mapper<LongWritable,Text,Text,Text>
{
    public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
    {
        for (String token: value.toString().split("\\s+")) {
            word.set(token);
            context.write(word, ONE);
    }
}
