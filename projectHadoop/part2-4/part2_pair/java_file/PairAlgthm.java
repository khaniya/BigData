package pairalgorithm;

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
        
public class PairAlgthm {
        
	public static class Map extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] arr = value.toString().split("\\s+");
			
			for (int i = 0; i < arr.length; i++) {
				for (int j = i + 1; j < arr.length && !arr[i].equals(arr[j]); j++){
					Text k = new Text(arr[i]);
					Text v = new Text(arr[j]);
					Text star = new Text("*");
					
					PairWritable pair1 = new PairWritable(k, star);
					PairWritable pair2 = new PairWritable(k, v);
					
					context.write(pair1, new IntWritable(1));
					context.write(pair2, new IntWritable(1));
				}
			}
		}		
	}
	
	public static class Reduce extends Reducer<PairWritable, IntWritable, PairWritable, Text> {
		
		//int total = 0;
		int total;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			total = 0;
		}

		@Override
		protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			
			if (key.getValue().toString().equals("*")) {
				total = sum;
			} else {
				String freq = String.format("%d/%d", sum, total);
				context.write(key, new Text(freq));
			}
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
     
	Job job = new Job(conf, "PairAlgthm");
    job.setJarByClass(PairAlgthm.class);
    
    job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(PairWritable.class);
	job.setOutputValueClass(Text.class);

        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}