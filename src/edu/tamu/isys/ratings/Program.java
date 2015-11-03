package edu.tamu.isys.ratings;

/*
 * Author Details:-
 * Author - Aditya Purandare 
 * UIN-724006256
 * Program - MS-Management Information Systems
 * Course - ISYS 622 Advanced Database Management Systems
 * Section - 603
 * Email - adityapurandare@tamu.edu
 * 
 * Execution Query:-
 * bin/hadoop jar 724006256.jar edu.tamu.isys.ratings.Program input output
 * Run Successful - 11/02/2015 11:59pm
 * 
 * Mapper Result Testing with zero reduce tasks,
 * bin/hadoop jar 724006256.jar edu.tamu.isys.ratings.Program -D mapred.reduce.tasks=0 input output
 * Test successful - 11/02/2015 6:26pm
 * 
 * Build History
 * Program Code Complete - 11/02/2015 8:29pm
 * Program optimized - 11/03/2015 10:14am
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
		//Console message for program initiation
		System.out.println("Program initializing");
		
		int res = ToolRunner.run(new Configuration(), new Program(), args);
		
		//System exits after posting console message of exit code informing user of the outcome
		System.out.println("Program ending with exit code: "+res);
		System.exit(res);
	}
	public int run (String args[]) throws Exception
	{
		//Following the latest Hadoop API by referring to Chapter 4 - Hadoop in Action & Apache Hadoop API reference
		//Job instantiated & Jar has been set by class as UIN-724006256
		Job job = Job.getInstance(getConf(), "724006256");
		job.setJarByClass(Program.class);
		
		//Setting the Mapper & Reducer classes to the custom ratings classes
		job.setMapperClass(RatingsMapper.class);
		job.setReducerClass(RatingsReducer.class);
		
		//Setting Input format class to read <LongWritable, Text>
		//Setting Input format class to write tab separated <Text, Text>
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Setting Mapper Output data classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Setting Reducer Output data classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Input & output directories have been captured to parse the input & post the output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Returns the job's success or failure to the main method
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
