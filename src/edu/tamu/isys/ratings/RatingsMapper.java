package edu.tamu.isys.ratings;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, Text>
{
	//For efficiency of the code, variables declared outside to optimize program
	private String data = "";
	private Text newKey;
	private Text newValue;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		data = value.toString();
		try 
		{
			MovieLogEntry entry = new MovieLogEntry(data);
			if(!entry.hasError())
			{
				newKey = new Text(entry.getGenre());
				newValue = new Text(entry.getName()+entry.getRating());
				context.write(newKey, newValue);
			}
		} 
		catch (ParseException e) {
			e.printStackTrace();
		}
		
	}
}
