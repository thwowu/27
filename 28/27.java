package ecp.Lab1.WordCount;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


import org.apache.hadoop.fs.FSDataInputStream;


public class Question27 {

    public static void main(String[] args) throws IOException  {

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
				String[] country = line.split(";");
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
		
    	/*
        // String csvFile = "/home/cloudera/BigData/Lab1/arbres.csv";
        String csvFile = args[0];
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ";";

        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] country = line.split(cvsSplitBy);

                System.out.println("Tree' " + country[11] + " , Year =" + country[5]  + " , height =" + country[6]);

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
*/
    }

}
