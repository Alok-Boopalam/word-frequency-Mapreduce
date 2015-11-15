import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class finalmr {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{


    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	 ArrayList<String> array = new ArrayList<String>();
    	 FileSystem hdfs =FileSystem.get(new Configuration());
    	 Path homeDir=hdfs.getHomeDirectory();
    	 //Scanner scanner1 = new Scanner(new FileReader("hdfs://localhost:54310/user/alok/output1/part-00000"));
    	 Path newFilePath = new Path(homeDir+"/output1/part-00000");
    	 BufferedReader bfr = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
    
    	 String str1 = null;
    	 while ((str1 = bfr.readLine())!= null) {
             String[] columns = str1.split("\t");
             
             array.add(columns[1]);
            // System.out.println(columns[1]);
         }
    	
    	 FileSystem hdfs1 =FileSystem.get(new Configuration());
    	 Path homeDir1=hdfs1.getHomeDirectory();
    	 //Scanner scanner1 = new Scanner(new FileReader("hdfs://localhost:54310/user/alok/output1/part-00000"));
    	 Path newFilePath1 = new Path(homeDir1+"/output2/part-r-00000");
    	 BufferedReader bfr1 = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath1)));
        HashMap<String, String> values = new HashMap<String, String>();
        String str2 = null;
   	 while ((str2 = bfr1.readLine())!= null) {
            String[] columns = str2.split("\t");
            if(!values.containsKey(columns[0]))
            {
            	values.put(columns[0], columns[1]);
            }
        }
        
       
  //System.out.print(array);
    	
    	StringTokenizer itr = new StringTokenizer(value.toString());
        int count = 0;
        double prod = 1;
        while (itr.hasMoreTokens()) {
      	  count++;
      	  String str = itr.nextToken()+","+count;
      //System.out.println(str);
      	double occurrences =0;
      	for (String temp : array) {
      		int x = Integer.parseInt(temp);
      		if (x>=count)
      		{
      			occurrences++;
      		}
		}
      	String repeattimes = values.get(str); 
      	Double times = Double.parseDouble(repeattimes);
       
       //System.out.println(occurrences);
        //System.out.println(times);
       double r=times/occurrences;
       prod = prod*r;
 
        }
     //   System.out.println(prod);
        String str = value.toString();
        word.set(str);
       // System.out.println(str);
        final DoubleWritable one = new DoubleWritable(prod);
        context.write(word,one);
      
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, double values,
                       Context context
                       ) throws IOException, InterruptedException {
     
     
       
     
      result.set(values);
      context.write(key, result);
    
    }
  }

  public static void finalpartpr(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = new Job(conf, "word count");
    job.setJarByClass(finalmr.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileSystem hdfs =FileSystem.get(new Configuration());
    Path homeDir=hdfs.getHomeDirectory();
    Path newFolderPath= new Path(homeDir+"/output3");
    if(hdfs.exists(newFolderPath))
    {
          //Delete existing Directory
          hdfs.delete(newFolderPath, true);
          System.out.println("Existing Folder Deleted.");
    }
     
     FileOutputFormat.setOutputPath(job, newFolderPath);
    
    
    
    job.waitForCompletion(true);
    
    FileSystem hdfs1 =FileSystem.get(new Configuration());
	 Path homeDir1=hdfs1.getHomeDirectory();
	 //Scanner scanner1 = new Scanner(new FileReader("hdfs://localhost:54310/user/alok/output1/part-00000"));
	 Path newFilePath1 = new Path(homeDir1+"/output3/part-r-00000");
	 BufferedReader bfr1 = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath1)));
    HashMap<String, Double> out = new HashMap<String, Double>();
    TreeMap<String, Double> finalout1 = new TreeMap<String, Double>();
    String str2 = null;
  	 while ((str2 = bfr1.readLine())!= null) {
        String[] columns = str2.split("\t");
        double aDouble = Double.parseDouble(columns[1]);
        out.put(columns[0],aDouble);
        }
    ArrayList as = new ArrayList( out.entrySet() );
    
    Collections.sort( as , new Comparator() {
        public int compare( Object o1 , Object o2 )
        {
            Map.Entry e1 = (Map.Entry)o1 ;
            Map.Entry e2 = (Map.Entry)o2 ;
            Double first = (Double)e1.getValue();
            Double second = (Double)e2.getValue();
            return second.compareTo( first );
        }
    });
    
    Path newFilePath=new Path(newFolderPath+args[1]);

    hdfs.createNewFile(newFilePath);
    StringBuilder sb=new StringBuilder();
   for(int i =0;i<3;i++)
   {
	   System.out.println(as.get(i));
	   sb.append(as.get(i));
       
       sb.append("\n");
       
       
   }
   
   byte[] byt=sb.toString().getBytes();
   FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
   fsOutStream.write(byt);

   fsOutStream.close();
    
  }
}