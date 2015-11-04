package edu.tamu.isys.ratings;

import java.text.ParseException;

/* Define a class for the Movie Ratings Problem */
public class MovieLogEntry {
	
	/* Creating & Initializing attributes within the class */
	private String MovieId= "";
	private String Name= "";
	private String Genre= "";
	private String UserId= "";
	private String UserAge= "";
	private String UserGender= "";
	private String Rating= "";
	private String Timestamp= "";
	private String errorMessage = "";
	
	/* Defining default constructor for RawData parsing */
	public MovieLogEntry (String rawData) throws ParseException
	{
		try
		{
			/* Puts input values to attributes */
			String[] parts = rawData.split("::");
			this.MovieId = parts[0].trim();
			this.Name=parts[1].trim();
			this.Genre=parts[2].trim();
			this.UserId=parts[3].trim();
			this.UserAge=parts[4].trim();
			this.UserGender=parts[5].trim();
			this.Rating=parts[6].trim();
			this.Timestamp=parts[7].trim();
		}
		catch (Exception e)
		{
			errorMessage = e.getStackTrace().toString()+":"+rawData;
			System.out.println(errorMessage);
		}
	}
	
	/* Method to return Movie ID */
	public String getMovieId()
	{
		return MovieId;
	}
	
	/* Method to return Movie Name */
	public String getName()
	{
		return Name;
	}
	
	/* Method to return Movie Genres as a single line [e.g. "Action, Drama, War"] */
	public String getGenre()
	{
		return Genre;
	}
	
	/* Method to return Movie Genres as an Array [e.g. {"Action","Drama", "War"}] */
	public String[] getGenres()
	{
		int loop_var;
		String[] genreList = Genre.split(",");
		
		/* Code optimized for efficiency by using ++loop_var to speed up for loop processing*/
		for(loop_var=0; loop_var < genreList.length; ++loop_var)
		{
			genreList[loop_var] = genreList[loop_var].trim();
		}
		return genreList;
	}
	
	/* Method to return  Movie Reviewer's ID */
	public String getUserId()
	{
		return UserId;
	}
	
	/* Method to return Movie Reviewer's Age */
	public String getUserAge()
	{
		return UserAge;
	}
	
	/* Method to return Movie Reviewer's Gender */
	public String getUserGender()
	{
		return UserGender;
	}
	
	/* Method to return Movie Rating */
	public String getRating()
	{
		return Rating;
	}
	
	/* Method to return Movie Review Time Stamp */
	public String getTimestamp()
	{
		return Timestamp;
	}
	
	/* Method to return Error Message as string */
	public String getError()
	{
		return errorMessage;
	}
	
	/* Method to return the status of Error Message Code */
	public boolean hasError()
	{
		return !errorMessage.isEmpty();
	}
}
