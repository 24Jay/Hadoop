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
		year = record.substring(15,19);
		String airTempString;

		if (record.charAt(87) == '+')
		{
			airTempString = record.substring(88, 92);
		}
		else
		{
			airTempString = record.substring(87, 92);
		}
		airTemperature = Integer.parseInt(airTempString);
		quality = record.substring(92, 93);
	}

	public boolean isMalformed()
	{
		if (record.length() < 93)
		{
			return true;
		}
		return false;
	}

	public boolean isMissing()
	{
		return airTemperature == MISSING_TEMPERATURE;
	}

	public boolean isValidTemperature()
	{
		return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
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
