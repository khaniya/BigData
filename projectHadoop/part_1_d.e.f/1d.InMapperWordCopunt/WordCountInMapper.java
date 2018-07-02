import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCountInMapper {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    //private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();
	 HashMap<String, Integer> hashMap;
	 
	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {
		 hashMap = new HashMap<>();
	 }
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	for (String token : value.toString().split("\\s+")) {
			if (token.length() == 0) continue;
			if (hashMap.containsKey(token)) {
				int newVal = hashMap.get(token) + 1;
				hashMap.put(token, newVal);
			} else {
				hashMap.put(token, 1);					
			}
		}
    }
    
    @Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(String key : hashMap.keySet()){
			context.write(new Text(key), new IntWritable(hashMap.get(key)));
		}
	}
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    FileSystem fs = FileSystem.get(conf);
	
	/*Check if output path (args[1])exist or not*/
	if(fs.exists(new Path(args[1]))){
	 /*If exist delete the output path*/
	 fs.delete(new Path(args[1]), true);
	}
     
	Job job = new Job(conf, "WordCount");
    job.setJarByClass(WordCountInMapper.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}