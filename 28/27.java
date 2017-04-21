package ecp.Lab1.WordCount;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class Question28 {

	public static void main(String[] args) throws IOException {
		
		Path filename = new Path(args[0]);
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				String[] country = line.split(;);
        System.out.println("Tree: " + country[11] + ", year=" + country[5] + ", height=" + country[6]);

				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}

		
		
	}

}
