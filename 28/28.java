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
			
      int line_counter = 0;
			String line = br.readLine();
			while (line !=null){
        if (line_counter > 21){  
        String[] country = line.split(;);
        System.out.println("Tree: " + country[11] + ", year=" + country[5] + ", height=" + country[6]);
        line_counter += 1
        }
        else{
        line_counter += 1
        }
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
