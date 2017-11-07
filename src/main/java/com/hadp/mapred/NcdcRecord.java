package com.hadp.mapred;

import org.apache.hadoop.io.Text;

public class NcdcRecord
{
	private static final int MISSING_TEMPERATURE = 9999;

	private String record;

	private String year;
	private int airTemperature;
	private String quality;
	private String station;

	private boolean malformed = false;

	public NcdcRecord(Text record)
	{
		this.record = record.toString();
		parse(this.record);
	}

	public void parse(String record)
	{
		if (isMalformed())
			return;

		station = record.substring(4, 15);
		year = record.substring(15, 19);
		String airTempString;

		if (record.charAt(87) == '+')
		{
			airTempString = record.substring(88, 92);
		}
		else
		{
			airTempString = record.substring(87, 92);
		}

		try
		{
			airTemperature = Integer.parseInt(airTempString);

		}
		catch (NumberFormatException e)
		{
			malformed = true;
		}
		quality = record.substring(92, 93);
	}

	public boolean isMalformed()
	{
		if (record.length() < 93)
			malformed = true;

		return malformed;
	}

	public boolean isMissing()
	{
		return airTemperature == MISSING_TEMPERATURE;
	}

	public boolean isValidTemperature()
	{
		return (!isMalformed()) && (!isMissing()) && quality.matches("[01459]");
	}

	public String getYear()
	{
		return year;
	}

	public int getAirTemperature()
	{
		return airTemperature;
	}

	public String getStation()
	{
		return station;
	}

	public String getQuality()
	{
		return quality;
	}
}
