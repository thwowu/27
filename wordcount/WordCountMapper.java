import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    public void map(LongWritable key, Text value, Context context) 
           throws IOException,InterruptedException{
        for (String token: value.toString().split("\\s+")) { 
            word.set(token);
            context.write(word, ONE);
    }
}
