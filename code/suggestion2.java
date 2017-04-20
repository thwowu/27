http://stackoverflow.com/questions/32259742/mapreduce-reading-two-more-columns-from-text-file


# Mapper

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Exercisemapper extends Mapper<LongWritable,Text,Text,Text>
{
    public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
    {
        String orig_val=value.toString();
        String[] orig_val1=orig_val.split(",");
        String state_val=orig_val1[0];
        String other_counts=orig_val1[2]+","+orig_val1[3]+","+orig_val1[4];
        context.write(new Text(state_val),new Text(other_counts));
    }


}



# Reducer

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExerciseReducer extends Reducer<Text,Text,Text,Text> 
{
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {
        Map<Integer,Integer> mymap=new HashMap<Integer,Integer>();
        StringBuilder sb=new StringBuilder();
        int myval=0;
        for(Text s:value)
        {
            String comma_values=s.toString();
            String[] comma_values_arr=comma_values.split(",");
            for(int i=0;i<comma_values_arr.length;i++)
            {
                if(mymap.get(i)==null)
                mymap.put(i,Integer.parseInt(comma_values_arr[i]));
                else
                {
                 myval=mymap.get(i)+Integer.parseInt(comma_values_arr[i]);
                 mymap.put(i,myval);
                }
            }
        }
        for(Integer finalval:mymap.values())
        {

            sb.append(finalval.toString());
            sb.append("\t");
        }
        context.write(key,new Text(sb.toString().replaceAll("\t$","")));        
    }
}


# Driver

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ExerciseDriver {
    public static void main(String args[]) throws Exception
    {
        if(args.length!=2)
            {
            System.err.println("Usage: Worddrivernewapi <input path> <output path>");
            System.exit(-1);
            }
        Job job=new Job();

        job.setJarByClass(ExerciseDriver.class);
        job.setJobName("ExerciseDriver");

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(Exercisemapper.class);

        job.setReducerClass(ExerciseReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}


