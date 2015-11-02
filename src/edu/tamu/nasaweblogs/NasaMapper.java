package edu.tamu.nasaweblogs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

//Reconsider Mapper Input
public class NasaMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>
{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		try{
			
			final IntWritable one = new IntWritable(1);
			String line = value.toString();
			String[] parts = line.split(":"); 
			
			//Checking Stepwise Parts
			//for(String word : parts)
			//	System.out.println(word);
			if(parts.length <= 4)
			{
				int hour=Integer.parseInt(parts[1]);
				final IntWritable hourInt = new IntWritable(hour);
				context.write(hourInt, one);
			}
			else
			{
				//System.out.println(line);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
