package edu.tamu.isys.ratings;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingsReducer extends Reducer<Text, Text, Text, Text> 
{
	private Text newValue;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String TopMovieName="";
		String TopMovieRating="";
		String FORMATTER="";
		
		Map<String, String> MovieMap = buildMap(values);
		Map<String, Double> AverageMovieMap = getAverageRating(MovieMap);
		Map<String, Double> SortedMap = sortByValue(AverageMovieMap);
		
		String[] parts = getLast(SortedMap);
		TopMovieName = parts[0];
		TopMovieRating = parts[1];
		
		FORMATTER = TopMovieName + " (" + TopMovieRating + ")";
		newValue = new Text(FORMATTER);
		
		context.write(key, newValue);
	}
	private String[] getLast(Map <String, Double> SortedMap)
	{
		String[] resultSet={"",""};
		for(String key : SortedMap.keySet())
		{
			resultSet[0] = key;
			resultSet[1] = Double.toString(SortedMap.get(key));
		}
		return resultSet;
	}
	private Map <String, Double> getAverageRating(Map <String, String> MovieMap)
	{
		String MovieMapValue="";
		String[] parts;
		int countOfRatings=0;
		int sumOfRatings=0;
		double averageRating=0d;
		
		Map <String, Double> averagemoviemap = new HashMap <String, Double>();
		
		for(String key : MovieMap.keySet())
		{
			MovieMapValue = MovieMap.get(key);
			parts = MovieMapValue.split(" ");
			
			countOfRatings = Integer.parseInt(parts[0]);
			sumOfRatings = Integer.parseInt(parts[1]);
			
			averageRating = sumOfRatings/countOfRatings; 
			
			averagemoviemap.put(key, averageRating);
		}
		return averagemoviemap;
	}
	private Map <String, String> buildMap (Iterable<Text> values)
	{
		String movieName="", movieRating="", countString="", sumString="";
		String lineText="";
		String moviecheck="";
		String[] parts, parter;
		int countOfRatings=0, sumOfRatings=0;
		
		Map<String, String> moviemap = new HashMap<String, String>();
		
		for(Text value : values)
		{
			lineText = value.toString();
			parts = lineText.split(" ");
			movieName=parts[0];
			movieRating=parts[1];
			
			moviecheck=moviemap.get(movieName);
			if(moviecheck == null)
			{
				countOfRatings = 1;
				sumOfRatings = Integer.parseInt(movieRating);
			}
			else
			{
				parter = moviecheck.split(" ");
				countOfRatings = Integer.parseInt(parter[0]) + 1;
				sumOfRatings = Integer.parseInt(parter[1]) + Integer.parseInt(movieRating);
			}
			countString=Integer.toString(countOfRatings);
			sumString=Integer.toString(sumOfRatings);
			moviemap.put(movieName, countString + sumString);
		}
		return moviemap;
	}
	
	public static Map <String, Double> sortByValue(Map <String, Double> unsortedMap) 
	{	 
		List<String> list = new LinkedList(unsortedMap.entrySet());
	 
		Collections.sort(list, new Comparator() 
		{
			public int compare(Object o1, Object o2) 
			{
				return ((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
			}
		});
	 
		Map sortedMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}