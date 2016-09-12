package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class ReplaceDateMain {

	public static void main(String[] args) 
	{
		InputStreamReader isr = null;
		BufferedReader br = null;
		try 
		{
			File dateFile = new File("C:\\Users\\Administrator\\Desktop\\TD历史数据备份\\未导\\TB_EVT_AIRLINE_INFO.txt");
			isr = new InputStreamReader(new FileInputStream(dateFile), "UTF-8");
			br = new BufferedReader(isr);
			String line = "";
			while ((line = br.readLine()) != null)
			{
			}
			
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				if (br != null)
					br.close();
				if (isr != null)
					isr.close();
			}
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
	}

}
