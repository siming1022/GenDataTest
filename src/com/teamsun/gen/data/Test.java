package com.teamsun.gen.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class Test {

	static final String PATH = "C:\\Users\\Administrator\\Desktop\\数据转换\\";
	
	public static void main(String[] args) throws Exception 
	{
		Date d = new Date();
//		DateFormat df = new SimpleDateFormat("HHmmss");
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
//		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		String ds = df.format(d);
		
		System.out.println(df.format(new Date()).substring(8, 14));
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
