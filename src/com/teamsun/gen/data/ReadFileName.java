package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadFileName 
{
	public static void main(String[] args) throws Exception
	{
		File dir = new File("D://bigData//导数据");
		
		try 
		{
			File[] fs = dir.listFiles();
			
			for (File f : fs)
			{
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
}
