package edu.tamu.isys.ratings;

/*
 * Author Details:-
 * Author - Aditya Purandare 
 * UIN-724006256
 * 
 * Execution Query:-
 * bin/hadoop jar 724006256.jar edu.tamu.isys.ratings.Program input output
 * 
 * For testing with zero reduce tasks,
 * bin/hadoop jar 724006256.jar edu.tamu.isys.ratings.Program -D mapred.reduce.tasks=0 input output
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Program extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		System.out.println("Program initializing");
		
		int res = ToolRunner.run(new Configuration(), new Program(), args);
		
		System.out.println("Program ending with exit code: "+res);
		System.exit(res);
	}
	public int run (String args[]) throws Exception
	{
		Job job = Job.getInstance(getConf(), "724006256");
		job.setJarByClass(Program.class);
		
		job.setMapperClass(RatingsMapper.class);
		job.setReducerClass(RatingsReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
