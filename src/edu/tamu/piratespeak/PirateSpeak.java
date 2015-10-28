package edu.tamu.piratespeak;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PirateSpeak extends Configured implements Tool
{	
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new PirateSpeak(), args);
		System.exit(res);
	}
	public int run (String args[]) throws Exception
	{
		Job job = Job.getInstance(getConf(), "piratespeak");
		job.setJarByClass(PirateSpeak.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		private final static IntWritable one = new IntWritable(1);
		
		public void reduce (IntWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String lineText="";
			String currentWord="";
			StringTokenizer itr = new StringTokenizer(line, " \t\n\r\f,.:;?![]'");
			
			while(itr.hasMoreTokens())
			{
				currentWord=itr.nextToken();
				lineText += WordReplacer(currentWord);
				//LOG.info(currentWord+":"+WordReplacer(currentWord));
			}
			Text result = new Text(lineText);
			context.write(one, result);
		}

		private static String WordReplacer(String word) 
		{
			if(word.equals("your")==true)
				word="yo";
			else if (word.equals("you")==true)
				word="yo";
			return word;
		}
	}
	public static class Map extends Mapper <LongWritable, Text, IntWritable, Text>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text lineText;
		
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			lineText = new Text(line);
			context.write(one, lineText);
		}
	}
}
