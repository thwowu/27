package ecp.Lab1.WordCount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank
{
	public static void main(String[] args) throws Exception
	{
		String input;
		String output;
		int threshold = 0;
		int iteration = 0;
		int iterationLimit = 100;
		boolean status = false;
		
		while(iteration < iterationLimit)
		{
			if((iteration % 2) == 0)
			{
				input = "input";
				output = "output";
			}else{
				input = "input";
				output = "output";
			}

			Configuration conf = new Configuration();	
			
			FileSystem fs = FileSystem.get(conf);
		      	if (fs.exists(new Path(output) ) ) {
				fs.delete(new Path(output), true);
				}
			
			Job job = Job.getInstance(new Cluster(conf));
			job.setJobName("PageRank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			TextInputFormat.addInputPath(job, new Path(input));
			TextOutputFormat.setOutputPath(job, new Path(output));
			job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
			status = job.waitForCompletion(true);
			iteration++;
			
			long count = job.getCounters().findCounter(PageCount.Count).getValue();
			long total_pr = job.getCounters().findCounter(PageCount.TotalPR).getValue();
			System.out.println("PageCount:"+count);
			System.out.println("TotalPR:"+total_pr);
			double per_pr = total_pr/(count*1.0d);
			System.out.println("Per PR:"+per_pr);
			
			// if the total Page Rank value is the same as the last time, 
		    // the loop will break to stop and spit out the result
			if((int)per_pr == threshold)
			{
				System.out.println("Iteration:"+iteration);
				break;
			}
			threshold = (int) per_pr;
		}
		System.exit(status?0:1);
	}
	
	static enum PageCount{
		Count,TotalPR
	}
	public static class PageRankMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			context.getCounter(PageCount.Count).increment(1);
			String[] inter = value.toString().split("\t");
			String firstnode = inter[0];
			String rankwithnode = inter[1];
			String spil[] = rankwithnode.split(" ");
			String pr = spil[0];
			String link = spil[1];
			context.write(new Text(firstnode), new Text(link));

			String linkingTo[] = link.split(",");
			float score = Float.valueOf(pr)/(linkingTo.length)*1.0f;

			// for loop write spit out all the connected nodes to the first node
			for(int i = 0 ; i < linkingTo.length ; i++)
			{
				context.write(new Text(site[i]), new Text(String.valueOf(score)));
			}	

		}
	}
 
	public static class PageRankReducer extends Reducer<Text, Text, Text, Text>
	{
		StringBuilder sb = new StringBuilder();
		// background setting 
		float factor = 0.85f;
		float pr = 0f;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			for(Text f : values)
			{				
				String value = f.toString();
				int s = value.indexOf(".");
				if(s != -1)
				{
					pr += Float.valueOf(value);
				}else{
					String site[] = value.split(",");
					int _len = site.length;
					for(int k = 0 ; k < _len ;k++)
					{
						sb.append(site[k]);
						sb.append(",");
					}
				}
			}

			pr = ((1-factor)+(factor*(pr)));
			context.getCounter(PageCount.TotalPR).increment((int)(pr*1000));
			String output = pr+","+sb.toString();
			context.write(key, new Text(output));
		}
	}	
}
