package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GenZhengZe
{
	public static void main(String[] args)
	{
		File dir = new File("D:\\bigData\\导数据");
		
		try 
		{
			File[] fs = dir.listFiles();
			
			for (File f : fs)
			{
				InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
				BufferedReader br = new BufferedReader(isr);
				String line = "";
				String patternStr = f.getName().toLowerCase().replaceAll(".txt", "").toUpperCase()+"=";
				int i = 0;
				int sum = 0;
				while ((line = br.readLine()) != null) 
	            {
					String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
					
					if ("DISTCD".equals(cols[0].toUpperCase()) || "DFILE_NAME".equals(cols[0].toUpperCase())
							|| "DATADT".equals(cols[0].toUpperCase()) || "LOADDT".equals(cols[0].toUpperCase()))
						continue;
					
					sum += Integer.parseInt(getLength(cols[1]));
					
					patternStr += "([\\s\\S]{" + getLength(cols[1]) + "})";
//					i += Integer.parseInt(getLength(cols[1]));
//					System.out.println(cols[1] + ": " + i);
	            }
				
//				System.out.println(f.getName().toLowerCase().replaceAll(".txt", "").toUpperCase() + ": " + sum);
				System.out.println(patternStr);
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	private static String getLength(String string) 
	{
		Pattern p = Pattern.compile("\\d+");
		Matcher m = p.matcher(string);
		while (m.find())
		{
			return m.group();
		}
		
		return "0";
	}
}
