package edu.tamu.nasaweblogs;

/*Execution Query - bear with me, cant type it everytime
bin/hadoop jar nasaweblogs.jar edu.tamu.nasaweblogs.NasaMain input output
Author -Aditya Purandare 10/26/2015 20:05:50
*/

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NasaMain extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		System.out.println("Program initializing");
		int res = ToolRunner.run(new NasaMain(), args);
		System.out.println("Program ending with exit code: "+res);
		System.exit(res);
	}
	public int run (String args[]) throws Exception
	{
		Job job = Job.getInstance(getConf(), "nasaweblogs");
		job.setJarByClass(NasaMain.class);
		
		job.setMapperClass(NasaMapper.class);
		job.setReducerClass(NasaReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
