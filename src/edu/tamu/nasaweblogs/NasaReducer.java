package edu.tamu.nasaweblogs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NasaReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
	private IntWritable result = new IntWritable();
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int sum=0;
		for (IntWritable value : values )
		{
			sum += value.get();
		}
		System.out.println(sum);
		result.set(sum/62);
		Text keyString= new Text(PrettyPrint(key.get()));
		Text resultString= new Text(result + " requests");
		context.write(keyString, resultString);
	}
	protected String PrettyPrint(int hour)
	{
		String message="";
		message=hour+":00 to "+(hour+1)+":00 has avg. traffic of";
		return message;
	}
}
