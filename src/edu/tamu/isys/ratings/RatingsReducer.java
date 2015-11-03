package edu.tamu.isys.ratings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingsReducer extends Reducer<Text, Text, Text, Text> 
{
	private Text newValue;
	String[] parts;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String TopMovieName="";
		String TopMovieRating="";
		String FORMATTER="";
		
		Map<String, String> MovieMap = buildMap(values);
		Map<String, Double> AveragedMovieMap = getAverageRating(MovieMap);
		
		parts = getTopMovie(AveragedMovieMap);
		TopMovieName = parts[0];
		TopMovieRating = parts[1];
		
		FORMATTER = TopMovieName + " (" + TopMovieRating + ")";
		newValue = new Text(FORMATTER);
		
		context.write(key, newValue);
	}
	private Map <String, String> buildMap (Iterable<Text> values)
	{
		String movieName="", movieRating="", countString="", sumString="";
		String lineText="";
		String[] parter;
		int countOfRatings=0, sumOfRatings=0;
		
		Map<String, String> moviemap = new HashMap<String, String>();
		
		for(Text value : values)
		{
			lineText = value.toString();
			parts = lineText.split("\\::");

			movieName=parts[0];
			movieRating=parts[1];
						
			if(moviemap.containsKey(movieName) == true)
			{
				parter = moviemap.get(movieName).split("/");

				countOfRatings = Integer.parseInt(parter[0]) + 1;
				sumOfRatings = Integer.parseInt(parter[1]) + Integer.parseInt(movieRating);
			}
			else
			{
				countOfRatings = 1;
				sumOfRatings = Integer.parseInt(movieRating);
			}
			
			countString=Integer.toString(countOfRatings);
			sumString=Integer.toString(sumOfRatings);
			moviemap.put(movieName, countString+"/"+sumString);
		}
		return moviemap;
	}
	
	private Map <String, Double> getAverageRating(Map <String, String> MovieMap)
	{
		String MovieMapValue="";
		
		double countOfRatings=0;
		double sumOfRatings=0;
		double averageRating=0d;
				
		Map <String, Double> averagemoviemap = new HashMap <String, Double>();
		
		for(String key : MovieMap.keySet())
		{
			MovieMapValue = MovieMap.get(key);
			parts = MovieMapValue.split("/");
			
			countOfRatings = Double.parseDouble(parts[0]);
			sumOfRatings = Double.parseDouble(parts[1]);
			
			averageRating = sumOfRatings/countOfRatings; 
			
			String s = String.format("%.2f", averageRating);
			averageRating = Double.parseDouble(s);
			
			averagemoviemap.put(key, averageRating);
		}
		return averagemoviemap;
	}
	
	private String[] getTopMovie(Map <String, Double> AveragedMovieMap)
	{
		String[] resultSet={"",""};
		String TopRatedMovie="";
		Double TopMovieRating=0d;
		Double keyValue=0d;
		
		for(String key : AveragedMovieMap.keySet())
		{
			keyValue = AveragedMovieMap.get(key);
			if(keyValue>TopMovieRating)
			{
				TopRatedMovie=key;
				TopMovieRating=keyValue;
			}
		}
		resultSet[0] = TopRatedMovie;
		resultSet[1] = Double.toString(TopMovieRating);
		return resultSet;
	}
}