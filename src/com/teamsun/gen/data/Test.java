package com.teamsun.gen.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

public class Test {

	public static void main(String[] args) throws Exception 
	{
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, 2014);
		c.set(Calendar.MONTH, 00);
		c.set(Calendar.DAY_OF_MONTH, 01);
		
		Calendar c2 = Calendar.getInstance();
		c2.set(Calendar.YEAR, 2014);
		c2.set(Calendar.MONTH, 11);
		c2.set(Calendar.DAY_OF_MONTH, 31);
		
		System.out.println(c.getTime().toLocaleString());
		System.out.println(c2.getTime().toLocaleString());
	}


	public static String encode(Dto dto)
	{
		String str = "";
		ByteArrayOutputStream baos = null;
		ObjectOutputStream oos = null;
		
		try 
		{
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(dto);
			oos.flush();
			
			str = baos.toString("ISO-8859-1");
			str = URLEncoder.encode(str, "UTF-8");
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				oos.close();
				baos.close();
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		
		return str;
	}
	
	private static Dto decode(String dtoStr)
	{
		Dto dto = null;
		ObjectInputStream in = null;
		try 
		{
			dtoStr = URLDecoder.decode(dtoStr, "UTF-8");
			in = new ObjectInputStream(new ByteArrayInputStream(dtoStr.getBytes("ISO-8859-1")));
			
			dto = (Dto) in.readObject();
			
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				in.close();
			}
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		
		return dto;
	}
}
