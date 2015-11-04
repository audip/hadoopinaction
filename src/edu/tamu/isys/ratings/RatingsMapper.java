package edu.tamu.isys.ratings;

/* Imports have been organized for RatingsMapper Class */
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, Text>
{
	/* For improving efficiency of the program, variables have been declared outside the methods to optimize the processing */
	private String rawData = "";
	private Text newKey;
	private Text newValue;
	private String[] genreList;
	private int loop_var;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		rawData = value.toString();
		
		/* Exception Handling by trying for uncatched exceptions */
		try 
		{
			/* Instantiated an object of MovieLogEntry Class */
			MovieLogEntry entry = new MovieLogEntry(rawData);
			
			/* Enforcing check for input to be clean, by checking for genre, rating, movie name
			 * The map method works correctly even if a userId, userAge, userGender, TimeStamp and Movie Id are missing
			 * as those details are trivial and the Movie Ratings problem can still be solved using the important attributes from Movie Log
			 */
			if(!entry.hasError() && !entry.getRating().isEmpty() && !entry.getGenre().isEmpty() && !entry.getName().isEmpty())
			{
				/* Returns list of genres for a given movie */
				genreList=entry.getGenres();
				
				/* Iterates genre for each movie and writes the result to Context
				 * For loop has been optimized for efficiency by following ++loop_var which speeds up the loop processing
				 */
				for(loop_var = 0; loop_var < genreList.length; ++loop_var)
				{
					/* Key is genre & Value is composite (Movie Name & Movie Rating) */
					newKey = new Text(genreList[loop_var]);
					newValue = new Text(entry.getName()+"::"+entry.getRating());
					
					/* Writing results to Context for key-value pairs */
					context.write(newKey, newValue);
				}
			}
		}
		/* Catches any unhandled exceptions and prints stack trace of the error message */
		catch (ParseException e) 
		{
			e.printStackTrace();
		}
	}
}
