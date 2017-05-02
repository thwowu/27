
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class MDP01A03 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new MDP01A03(), args);	
		System.exit(res);
		   }

	public static NumberFormat NF = new DecimalFormat("00");
   	public static Set<String> NODES = new HashSet<String>();
    	public static String LINKS_SEPARATOR = "|";
	
	@Override
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "MDP01A03");
	      job.setJarByClass(MDP01A03.class);

	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      job.setMapOutputKeyClass(Text.class);
              job.setMapOutputValueClass(Text.class);
		
	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);
	      job.setNumReduceTasks(1);
	      job.setCombinerClass(Reduce.class);
	      
	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
	      
	      FileSystem fs = FileSystem.newInstance(getConf());
	      if (fs.exists(new Path(args[1]))) {
				fs.delete(new Path(args[1]), true);
			}
	      
	      
	      job.waitForCompletion(true);
	      
	      return 0;
	}

	 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		 
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
		      if (value.charAt(0) != '#') {
            
            int tabIndex = value.find("\t");
            String nodeA = Text.decode(value.getBytes(), 0, tabIndex);
            String nodeB = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
            context.write(new Text(nodeA), new Text(nodeB));
            
            // add the current source node to the node list so we can 
            // compute the total amount of nodes of our graph in Job#2
            // adding to a HashSet so there will not be duplicate nodes to the list.

            PageRank.NODES.add(nodeA);


            // also add the target node to the same list: we may have a target node 
            // with no outlinks (so it will never be parsed as source)
            PageRank.NODES.add(nodeB);
            }
	      }
	   }

	   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	      
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	      boolean first = true;
	      String links = (PageRank.DAMPING / PageRank.NODES.size()) + " ";

	      for (Text value : values) {
		if (!first) 
		     links += ",";
		links += value.toString();
		first = false;}
		context.write(key, new Text(links)); }
	    }
   }

}
