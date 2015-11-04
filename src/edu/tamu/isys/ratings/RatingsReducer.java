package edu.tamu.isys.ratings;

/* Imports have been organized for RatingsReducer Class */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingsReducer extends Reducer<Text, Text, Text, Text> 
{
	/* For improving efficiency of the program, variables have been declared outside the methods to optimize the processing */
	private Text newValue;
	String[] parts;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		/* Declared variables for the reduce method */
		String TopMovieName="";
		String TopMovieRating="";
		String FORMATTER="";
		
		/* buildMap() method sets up the key-value to process the composite[movie name & rating] */
		Map<String, String> MovieMap = buildMap(values);
		
		/* getAverageRating() uses the built map to compute average rating for every movie in the dataset [movie name, average of ratings] */
		Map<String, Double> AveragedMovieMap = getAverageRating(MovieMap);
		
		/* getTopMovie() method takes input of the map [movie name, average rating] and performs comparisons to find 
		 * top rated movie of the genre
		 */
		parts = getTopMovie(AveragedMovieMap);
		TopMovieName = parts[0];
		TopMovieRating = parts[1];
		
		/*
		 * Formats the processed results to the output format before writing it to Context 
		 */
		FORMATTER = TopMovieName + " (" + TopMovieRating + ")";
		newValue = new Text(FORMATTER);
		
		/* Writes to context the key-value [genre, {top movie name (top movie avreage rating)}] pair in TextOutputFormat (tab-separated) */
		context.write(key, newValue);
	}
	private Map <String, String> buildMap (Iterable<Text> values)
	{
		String movieName="";
		String movieRating="";
		String countString="";
		String sumString="";
		String lineText="";
		
		String[] parter;
		
		int countOfRatings=0, sumOfRatings=0;
		
		/* Instantiated a Map to capture the results and perform the processing of values */
		Map<String, String> movieMap = new HashMap<String, String>();
		
		/* Iterates each value [movieName movieRating] in the list of movies for the particular genre*/
		for(Text value : values)
		{
			/* Splits the movie name & movie rating to two entities*/
			lineText = value.toString();
			parts = lineText.split("\\::");

			movieName=parts[0];
			movieRating=parts[1];
			
			/* Ensures the specification of Map data type are met by having unique keys and 
			 * computing count of review and sum of ratings for every movie 
			 */
			if(movieMap.containsKey(movieName) == true)
			{
				/* If movie name is already in the map, fetches the value store and updates it */
				parter = movieMap.get(movieName).split("/");

				countOfRatings = Integer.parseInt(parter[0]) + 1;
				sumOfRatings = Integer.parseInt(parter[1]) + Integer.parseInt(movieRating);
			}
			else
			{
				/* When movie is not already in the map, puts it into the map with initial count and rating */
				countOfRatings = 1;
				sumOfRatings = Integer.parseInt(movieRating);
			}
			
			/* Type conversion of the results to put it in the map before returning the results */
			countString=Integer.toString(countOfRatings);
			sumString=Integer.toString(sumOfRatings);
			movieMap.put(movieName, countString+"/"+sumString);
		}
		return movieMap;
	}
	
	private Map <String, Double> getAverageRating(Map <String, String> movieMap)
	{
		/* Initialized the values for the variables */
		String movieMapValue="";
		String doublePrecisionString="";
		
		double countOfRatings=0;
		double sumOfRatings=0;
		double averageRating=0d;
		
		/* Instantiated a map to capture [movie name, average rating] for the input map*/
		Map <String, Double> averageMovieMap = new HashMap <String, Double>();
		
		/* Iterates each key [movie name] in the keySet [list of movie names] of the map for computing average rating*/
		for(String key : movieMap.keySet())
		{
			/*Retrieves value for the key in movieMap & splits it into count of ratings & sum of ratings*/
			movieMapValue = movieMap.get(key);
			parts = movieMapValue.split("/");
			
			/* Enforce robustness by explicitly casting data types to Double*/
			countOfRatings = Double.parseDouble(parts[0]);
			sumOfRatings = Double.parseDouble(parts[1]);
			
			/* Computes the average rating for a particular movie */
			averageRating = sumOfRatings/countOfRatings; 
			
			/* Code snippet type casts the Double to String to round up the result to two digits after decimal point and casts it back into Double */
			doublePrecisionString = String.format("%.2f", averageRating);
			averageRating = Double.parseDouble(doublePrecisionString);
			
			/* Puts the computed key-value pair into the new averageMovieMap before returning it*/
			averageMovieMap.put(key, averageRating);
		}
		return averageMovieMap;
	}
	
	private String[] getTopMovie(Map <String, Double> averagedMovieMap)
	{
		String[] resultSet={"",""};
		String topRatedMovie="";
		
		Double topMovieRating=0d;
		Double keyValue=0d;
		
		/* Iterates every key[movie name] in the keySet [list of movies] to find the highest rated movie*/
		for(String key : averagedMovieMap.keySet())
		{
			/* Retrieves value for the particular key in the genre */
			keyValue = averagedMovieMap.get(key);
			
			/* Compares the current movie rating to the top rating iterated so far to find out the 
			 * highest rated pair [movie name, average movie rating]
			 */
			if(keyValue>topMovieRating)
			{
				topRatedMovie=key;
				topMovieRating=keyValue;
			}
		}
		
		/* Type casts the results to an array to be returned the map method */
		resultSet[0] = topRatedMovie;
		resultSet[1] = Double.toString(topMovieRating);
		return resultSet;
	}
}