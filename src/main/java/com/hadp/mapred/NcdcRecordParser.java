package com.hadp.mapred;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class NcdcRecordParser
{
	private static final int MISSING_TEMPERATURE = 9999;

	private String year;
	private int airTemperature;
	private String quality;

	public void parse(String record)
	{
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
		airTemperature = Integer.parseInt(airTempString);
		quality = record.substring(92, 93);
	}

	public void parse(Text record)
	{
		parse(record.toString());
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
}
