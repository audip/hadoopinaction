package edu.tamu.isys.ratings;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, Text>
{
	//For efficiency of the program, variables declared outside to optimize the processing
	private String data = "";
	private Text newKey;
	private Text newValue;
	private String[] genreList;
	private int loop_var;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		data = value.toString();
		//Exception Handling by trying for uncatched exceptions
		try 
		{
			//Instantiated an object of MovieLogEntry Class
			MovieLogEntry entry = new MovieLogEntry(data);
			
			//Enforcing check for input to be clean, by checking for genre, rating, movie name 
			if(!entry.hasError() && !entry.getRating().isEmpty() && !entry.getGenre().isEmpty() && !entry.getName().isEmpty())
			{
				//returns list of genres for a given movie
				genreList=entry.getGenres();
				
				//Iterate genre for a movie and write the results to Context
				//For loop has been optimized for efficiency by following ++loop_var which speeding up the loop processing
				for(loop_var = 0; loop_var < genreList.length; ++loop_var)
				{
					//Key is genre & Value is composite (Movie Name & Movie Rating)
					newKey = new Text(genreList[loop_var]);
					newValue = new Text(entry.getName()+"::"+entry.getRating());
					
					//Writing results to Context for key-value pairs
					context.write(newKey, newValue);
				}
			}
		} 
		catch (ParseException e) 
		{
			e.printStackTrace();
		}
	}
}
