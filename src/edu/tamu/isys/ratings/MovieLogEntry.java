package edu.tamu.isys.ratings;

import java.text.ParseException;

public class MovieLogEntry {
	
	private String MovieId= "";
	private String Name= "";
	private String Genre= "";
	private String UserId= "";
	private String UserAge= "";
	private String UserGender= "";
	private String Rating= "";
	private String Timestamp= "";
	private String errormsg = "";
	
	public MovieLogEntry (String rawData) throws ParseException
	{
		try
		{
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
			errormsg = e.getStackTrace().toString()+":"+rawData;
			System.out.println(errormsg);
		}
	}
	
	public String getMovieId()
	{
		return MovieId;
	}
	public String getName()
	{
		return Name;
	}
	public String getGenre()
	{
		return Genre;
	}
	public String[] getGenres()
	{
		int loop_var;
		String[] genreList = Genre.split(",");
		for(loop_var=0; loop_var < genreList.length; ++loop_var)
		{
			genreList[loop_var] = genreList[loop_var].trim();
		}
		return genreList;
	}
	public String getUserId()
	{
		return UserId;
	}
	public String getUserAge()
	{
		return UserAge;
	}
	public String getUserGender()
	{
		return UserGender;
	}
	public String getRating()
	{
		return Rating;
	}
	public String getTimestamp()
	{
		return Timestamp;
	}
	public String getError()
	{
		return errormsg;
	}
	public boolean hasError()
	{
		return !errormsg.isEmpty();
	}
}
