	
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
	
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class CorpusCalculator {

 public static class MapTheSentenceCount extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
   
     private Text word = new Text();
	
	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       String line = value.toString();
       StringTokenizer tokenizer = new StringTokenizer(line);
       IntWritable cnt = new IntWritable(tokenizer.countTokens());
	       word.set("wordCount");
	       
	         output.collect(word, cnt);
	     }
	   }

   public static class TheSentenceCountReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       int sum = 0;
	       while (values.hasNext()) {
	         sum = values.next().get();
	         output.collect(key, new IntWritable(sum));
	              
	              }
	       
       
	     }
  }
	   public static void main(String[] args) throws Exception {
		   
     JobConf conf = new JobConf(CorpusCalculator.class);
	     conf.setJobName("wordcount");
     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(IntWritable.class);
	
     conf.setMapperClass(MapTheSentenceCount.class);
	     conf.setCombinerClass(TheSentenceCountReduce.class);
	     conf.setReducerClass(TheSentenceCountReduce.class);

     conf.setInputFormat(TextInputFormat.class);
   conf.setOutputFormat(TextOutputFormat.class);
   FileInputFormat.setInputPaths(conf, new Path(args[0]));
   
   FileSystem hdfs =FileSystem.get(new Configuration());
   Path homeDir=hdfs.getHomeDirectory();
   Path newFolderPath= new Path(homeDir+"/output1");
   if(hdfs.exists(newFolderPath))
   {
         //Delete existing Directory
         hdfs.delete(newFolderPath, true);
         System.out.println("Existing Folder Deleted.");
   }
    
    FileOutputFormat.setOutputPath(conf, newFolderPath);

	     JobClient.runJob(conf);
	     
	     posAndCnt pnc = new posAndCnt();
	     pnc.posndCnt(args);
	     //TimeUnit.SECONDS.sleep(30);
	     finalmr fr = new finalmr();
	     fr.finalpartpr(args);
   }
}