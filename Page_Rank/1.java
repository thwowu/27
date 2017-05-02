
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

public class MDP01A03 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new MDP01A03(), args);	
		System.exit(res);
		   }

	@Override
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "MDP01A03");
	      job.setJarByClass(MDP01A03.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);
	      job.setNumReduceTasks(10);
	      job.setCombinerClass(Reduce.class);
	      
	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      
	      FileOutputFormat.setCompressOutput(job, true);
	      FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
	      // choices at page 129, Hadoop; Definitive Guide 4th edition
	      /* Worked:
	      org.apache.hadoop.io.compress.BZip2Codec
	      org.apache.hadoop.io.compress.GzipCodec
	      org.apache.hadoop.io.compress.DefaultCodec
	      
	      not working:
	      org.apache.hadoop.io.compress.SnappyCodec ("this version of libhadoop was built without " + "snappy support")
	      com.hadoop.compression.lzo.LzopCodec
	      org.apache.hadoop.io.compress.Lz4Codec
	      org.apache.hadoop.io.compress.DefaultCodec
	      */
	      
	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
	      
	      /* Delete output file Path if already exists 
	       * idea from http://unmeshasreeveni.blogspot.com/2014/04/code-for-deleting-existing-output.html */
	      FileSystem fs = FileSystem.newInstance(getConf());

	      if (fs.exists(new Path(args[1]))) {
				fs.delete(new Path(args[1]), true);
			}
	      // ends here
	      
	      
	      job.waitForCompletion(true);
	      
	      return 0;
	}

	 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	      private final static IntWritable ONE = new IntWritable(1);
	      private Text word = new Text();

	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	  for (String token: value.toString().split("\\s*\\b\\s*")) {
		        	 token = token.trim();
		        	 Pattern p = Pattern.compile("^[a-zA-Z0-9_]");
			    	 Matcher m = p.matcher(token.toLowerCase());
			    	 
		        	 if ( token.isEmpty() ) {
		                 continue;
		             } 
		            	 if (!m.find())
		            	 {
		            		 continue;}
		            word.set(token.toLowerCase());
		            context.write(word, ONE);
	         }
	      }
	   }

	   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	         int sum = 0;
	         for (IntWritable val : values) {
	            sum += val.get();
	         }
	         if (sum > 4000){
	        	 context.write(key, new IntWritable(sum));
	         }
	      }
	   }

}
