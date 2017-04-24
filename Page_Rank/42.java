// https://gist.github.com/RicherMans/7415618

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank {


	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, PRWritable> {
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, PRWritable> context, Reporter arg3)
				throws IOException {
			Pattern pat_title = Pattern.compile("&lttitle&gt(.+)&lt/title&gt");
			Pattern pat_links = Pattern.compile("\\[\\[(.+?)\\]\\]");
			StringTokenizer tok = new StringTokenizer(value.toString(), "\t");
			HashMap<String, List<String>> title_line = new HashMap<>();
			while (tok.hasMoreTokens()) {
				String tline = tok.nextToken();
				Matcher match = pat_title.matcher(tline);
				while (match.find()) {
					List<String> links = new ArrayList<>();
					// finding title , and every link in that title
					Matcher links_mat = pat_links.matcher(tline);
					while(links_mat.find()) {
						links.add(links_mat.group(1).toLowerCase());
					}
					title_line.put(match.group(1).toLowerCase(), links);
				}

			}
			int size_out = title_line.values().size();
			long page_rank = new java.util.Random().nextInt(100);
			if (size_out > 1) {
				page_rank = (key.get() / size_out);
			}
			Iterator<Entry<String, List<String>>> it = title_line.entrySet()
					.iterator();
			while (it.hasNext()) {
				Entry<String, List<String>> entry = it.next();
				context.collect(new Text(entry.getKey()), new PRWritable(
						page_rank, entry.getValue()));
			}
		}

		/*
		 * map ((url,PR), out_links) //PR = random at start for link in
		 * out_links emit(link, ((PR/size(out_links)), url))
		 * 
		 * 
		 */

	}

	public static class Reduce extends MapReduceBase
			implements
			org.apache.hadoop.mapred.Reducer<Text, PRWritable, LongWritable, Text> {

		@Override
		public void reduce(Text url, Iterator<PRWritable> weight_url,
				OutputCollector<LongWritable, Text> arg2, Reporter arg3)
				throws IOException {
			
			long pr = 0;
			while(weight_url.hasNext()){
				PRWritable pr_w = weight_url.next();
				pr = pr + pr_w.getPr().get();
			}
			
			arg2.collect(new LongWritable(pr), new Text(url));

		}
		

	}

	static String HDFS_PREFIX = "hdfs://localhost:9000";

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, InterruptedException {
		JobConf conf = new JobConf(PageRank.class);
		
		conf.setJobName("Pagerank");
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		if(args.length>2){
			HDFS_PREFIX = args[2];
		}
		Path p = new Path(HDFS_PREFIX + args[0]);

		FileInputFormat.setInputPaths(conf, p);
		FileOutputFormat.setOutputPath(conf, new Path(HDFS_PREFIX + args[1]
				+ new java.util.Random().nextInt()));

		JobClient.runJob(conf);
	}
}
