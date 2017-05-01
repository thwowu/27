package ecp.Lab1.WordCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class tfidf extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
			
	// part1----------------------------------------------------
	// 	
		
        Configuration conf1 = new Configuration();

        // counting the documents. Later used in setting the numbers of reducers 
        
        int Count = 0;
        FileSystem fs = FileSystem.get(conf1);
        RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(args[0]), false);
        while (ri.hasNext()){
            Count++;
            ri.next();
        }
        
        
        FileSystem fs1 = FileSystem.newInstance(getConf());
  		if (fs1.exists(new Path(args[1]))) {
  			fs1.delete(new Path(args[1]), true);
  		}
  	    FileSystem fs2 = FileSystem.newInstance(getConf());
  		if (fs2.exists(new Path(args[2]))) {
  			fs2.delete(new Path(args[2]), true);
  		}
        		
        Job job1 = Job.getInstance(conf1, "My_tdif_part1");
        job1.setJarByClass(tfidf.class);
        job1.setMapperClass(Mapper_Part1.class);
        job1.setCombinerClass(Combiner_Part1.class); 
        job1.setReducerClass(Reduce_Part1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(Count);
        
        // job1.setPartitionerClass(MyPartitoner.class); 
        /* control the way Hadoop partitions and sorts at reduce level 
	 * it offers the possibility of using a custom partitioning and group comparator. 
	 * We are going to use this in our last step to calculate tf-idf.
	 * 
	 * 
	 * Our partitioner will make sure that we partition 
	 * by the term itself only and not by the document id contained in the key.
	 * By this we achieve a fairly good distribution and the possibility to 
	 * count the occurrence of a term at the reducer.
	 * 
	 * http://henning.kropponline.de/2014/06/08/map-reduce-tf-idf/
	 */

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
        
        
        // part2----------------------------------------
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "My_tdif_part2");
        job2.setJarByClass(TFIDF.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(Mapper_Part2.class);
        job2.setReducerClass(Reduce_Part2.class);
        job2.setNumReduceTasks(p.length);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
	
     // hdfs.delete(new Path(args[1]), true);
		
    }
	
	public static void main(String[] args) throws Exception {
		tfidf tfidf = new tfidf();
        int res = ToolRunner.run(tfidf, args);
        System.exit(res);
	}
// part1------------------------------------------------------------------------
    public static class Mapper_Part1 extends
        Mapper<LongWritable, Text, Text, Text> {
            
        
        int all = 0; 
        // counting the numbers of individual words in one documents
        
        static Text one = new Text("1");
        
        
        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                          
String File_name = ((FileSplit) context.getInputSplit()).getPath().getName();
	        file = File_name;
	        
	         for (String token: value.toString().split("\\s*\\b\\s*")) {
	         	token = token.trim().toLowerCase();
	         	Pattern p = Pattern.compile("[A-Za-z0-9]");
	 	    	Matcher m = p.matcher(token);
	         	
	 	    	if (token.isEmpty() ) {
	                 continue;
	             } 
	            if (!m.find()){
	            continue;}
             StringTokenizer itr = new StringTokenizer(token.toString());            
            
             while (itr.hasMoreTokens()) {
                all++;
                itr.nextToken(); }      
	         }
	         
	         for (String token: value.toString().split("\\s*\\b\\s*")) {
		         	token = token.trim().toLowerCase();
		         	Pattern p = Pattern.compile("[A-Za-z0-9]");
		 	    	Matcher m = p.matcher(token);
		         	
		 	    	if (token.isEmpty() ) {
		                 continue;
		             } 
		            if (!m.find()){
		            continue;}
	            StringTokenizer itrr = new StringTokenizer(token.toString());
	            
	            
		        String word;
			    while (itrr.hasMoreTokens()) {
			    	word = File_name;
	                word += " ";
	                word += itrr.nextToken(); 
	                
	                context.write(new Text(word), one);}
		         }
                // key: document name + word 
		// value: 1 
		// example: doc1 hello 1
                //          doc1 newnewnew 1
            }
        }
	
	/* to initialize and clean up your map/reduce tasks
	 * (don't have access to any data from the input split directly)
	 * have a chance to do something before and after your map/reduce tasks
	 * 	 
	 * 1. clean up any resources you may have allocated
	 * 2. flush out any accumulation of aggregate results
	 * 
	 * word count example
	 * 
	 * want to exclude certain words from being counted (e.g. stop words such as "the", "a", "be", etc...)
	 * 1. pass a list (comma-delimited) of these words as a parameter (key-value pair) into the configuration object
	 * 2. in map code, during setup():
	 *    acquire the stop words and store them in some global variable (global variable to the map task)
	 *    exclude counting these words during your map logic
	 * 
	 * setup -> map -> cleanup
	 * setup -> reduce -> cleanup
	 *
	 * http://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for
	*/ 
	    
	// expecting to calculate
        public void cleanup(Context context) throws IOException,
                InterruptedException {

			
	    String str = "";
            str += all; // "all" is the total numbers of individual words in one document
            context.write(new Text(File_name + " " + "0"), new Text(str));
        }
	    
	/* map will flush the following output:
	 * key             value
	 * ---------------------
	 * doc1 !          499
	 * doc1 hello      1
	 * doc1 newnewnew  1
	 * doc1 ZEXX       1
	 * 
	 */
    }

   // setup (no use) -> mapper_1 -> cleanup -> combiner_1 (current) -> Partitioner -> reducer_1  
    // objective: complete term frequncy
    public static class Combiner_Part1 extends Reducer<Text, Text, Text, Text> {
        
	float all;
	String kk;
	    
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
		
	    // mission 1: get the number of total individiual words in a document----------------------
		
            int index = key.toString().indexOf(" ");
	    // get the position of the separator, 
	    // later to use value to extract the total individual words amount, marked as "!"
		
            if (key.toString().substring(index + 1, index + 2).equals("0")) {
            	kk = values.toString();
                for (Text val : values) {
                    all = Integer.parseInt(val.toString());
                    }
                // do not output this pair of key-value, avoid using it in later stream
		// only extract the number for calculation in combiner
                return;
            }
	    
	    // mission 2: calculate the frequency of "a single word" ----------------------------------
	    //                                               (locally)
	    // recycle the codes from Stanford word count
	    // the mulitiplication needs to use integer instead of String, use "Integer.parseInt()"
	
            float sum = 0; 
            for (Text val : values) { // previous "if" function  avoided the pairs from cleanup part
                sum += Integer.parseInt(val.toString());
            }
		
		
	    // mission 3: Term Frequncy of a single word, in a document ---------------------------------------------- 
            float tmp = sum / all;
            String value = "";
            value += tmp; 

            // switch the position between document file name and word
	    // ex: doc1 hello -> hello doc1
		
            String p[] = key.toString().split(" ");
            String key_to = "";
            key_to += p[1]; // word name : hello
            key_to += " ";
            key_to += p[0]; // file name : doc1
            context.write(new Text(key_to), new Text(value));
        }
	 /* combiner will flush the following output:
	 * key                value (Term Frequency)
	 * ------------------------------------
	 * hello doc1         0.5
	 * hello doc2         0.011
	 * hello doc3         0.002
	 * newnewnew doc1     0.2
	 * newnewnew doc2     0.211
	 * ZEXX doc3          0.4
	 * 
	 */
    }

   // setup (no use) -> mapper_1 -> cleanup -> combiner_1 -> Partitioner (current) -> reducer_1  ?????
    public static class MyPartitoner extends Partitioner<Text, Text> {

	    public int getPartition(Text key, Text value, int numPartitions) {

            String ip1 = key.toString();
            ip1 = ip1.substring(0, ip1.indexOf(" "));
            Text p1 = new Text(ip1);
            return Math.abs((p1.hashCode() * 127) % numPartitions);
        }
    }

    // setup (no use) -> mapper_1 -> cleanup -> combiner_1 -> Partitioner -> reducer_1 (current)
    // objective: bypass
    public static class Reduce_Part1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }


    // part2----------------------------------------------------- IDF
    // objective: acquire IDF & TF-IDF
	
    // from TF to IDF
    public static class Mapper_Part2 extends
            Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
	
	// LongWritable type, so the text type data pour into value 
	
            String val = value.toString().replaceAll("    ", " "); 
            // correct tab by space
 
	    // mission 1: extract the data for output use
	    /* 
	     * new key : word 
	     * new value: document file name + term frquency value
	    */ 
	    int index = val.indexOf(" ");
            String s1 = val.substring(0, index); 
            String s2 = val.substring(index + 1);
            s2 += " ";
            s2 += "1"; 
            context.write(new Text(s1), new Text(s2));
        }
	 /* map will flush the following output:
	 * key (word)         value (file name + TF + Count)
	 * ------------------------------------
	 * hello              doc1 0.5 1
	 * hello              doc2 0.011 1
	 * hello              doc3 0.002 1
	 * newnewnew          doc1 0.2 1
	 * newnewnew          doc2 0.211 1
	 * ZEXX               doc3 0.4 1
	 * 
	 */    
    }

    
    public static class Reduce_Part2 extends Reducer<Text, Text, Text, Text> {
        int file_count;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

	// Mission 1: IDF ------------------------------------------------------------

            // Get the # of documents in total (set by the configuration)
            file_count = context.getNumReduceTasks();

            float sum = 0;
            List<String> vals = new ArrayList<String>();

            for (Text str : values) {     // str <- values <- "doc1 0.5 1"
                int index = str.toString().lastIndexOf(" ");
		// use function "lastIndexOf" because there are two spaces
		// but we only want to take the last integer "1"
		
                sum += Integer.parseInt(str.toString().substring(index + 1)); 
		// keep adding up if appearing in several documents
		    
                vals.add(str.toString().substring(0, index)); 
		// document file name: where does this word appears? & its TF value
		// ex: [doc1 0.5, doc2 0.2122, doc5 0.11, doc6 0.209]
	        
            }
		
            double tmp = Math.log10( file_count * 1.0 /(sum * 1.0)); 
	    // IDF: term i appears in ni of the N documents in the collection
            
	    
	// Mission 2: TF-IDF ------------------------------------------------------------
		
	    if (set.size() < 0 || set.isEmpty() ){ return;}
            else {
            for (String val : set) {
            	

		        String end = val.substring(val.lastIndexOf("	")); // if taking infinity, it crashes
		        float f_end = Float.parseFloat(end);
		        val += " ";
		        //System.out.println("end string: " + end);
		        // System.out.println("idf: " + tmp);

		        
		        val += f_end * tmp;
		        context.write(key, new Text(val));
		         }	
               }
        }
	 /* 
	 * reducer2 will flush the following output:
	 * key (word)         value (file name + TF + IDF)
	 * ------------------------------------
	 * hello              doc1 0.5 1.21
	 * hello              doc2 0.011 8.2
	 * hello              doc3 0.002 3.1
	 * newnewnew          doc1 0.2 1 1.5
	 * newnewnew          doc2 0.211 3.0 
	 * ZEXX               doc3 0.4 1.44
	 * 
	 */    
    }
	
}
