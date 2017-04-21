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
			if (line_counter > 22){  
				processLine(line);
				
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
		
	public static void processLine(String line) {
	
	String USAF = line.substring(0, 6);	
	String name = line.substring(13, 42);
	String FIPS = line.substring(43, 45);
        String altitude = line.substring(74, 81);

        System.out.println("Name [" + name + "]\t Country [" + FIPS + "]\t Elevation [" + altitude +"]");
    }

}
