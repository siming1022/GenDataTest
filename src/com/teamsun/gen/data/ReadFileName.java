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
	static Map<String, String> tdTableInfos;
	public static void main(String[] args) throws Exception
	{
		File dir = new File("C:\\Users\\Administrator\\Desktop\\TD历史数据备份\\已导\\2014\\一般\\");
//		File dir = new File("C:\\Users\\Administrator\\Desktop\\TD历史数据备份\\未导\\");
//		File dir = new File("D:/bigData/导数据/");
		tdTableInfos = getTdTableInfo();
		swarpFile(dir);
	}
	
	public static void swarpFile(File file)
	{
		if (file.isDirectory())
		{
			File[] files = file.listFiles();
			for (File f : files)
			{
				swarpFile(f);
			}
		}
		else
		{
			String tableName = file.getName().toUpperCase().replace(".TXT", "");
//			System.out.println(file.getName());
//			System.out.println("hdfs dfs -du -h /EMS_Data/teamsun/" + file.getName().toUpperCase().replace(".TXT", ""));
//			System.out.println("hdfs dfs -count /EMS_Data/teamsun/" + file.getName().toUpperCase().replace(".TXT", ""));
			
			System.out.println(tableName);
//			String[] infos = tdTableInfos.get(tableName).split("~~");
//            String tdTableName = infos[0];
//            String dateCol = infos[1];
//            
//			System.out.println(tableName.trim() + " " + tdTableName.trim() + " " + dateCol.trim() + " a");
		} 
	}
	
	private static Map<String, String> getTdTableInfo()  throws Exception
	{
		Map<String, String> tdInfoMap = new HashMap<String, String>();
		File rowkeyFile = new File("D:\\bigData\\tableNames.txt");
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(rowkeyFile), "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		while ((line = br.readLine()) != null) 
        {
			String[] info = line.split("\\t");
			String name = info[0];
			String tbName = info[1];
			String whereCol = "";
			
			if (info.length > 2)
			{
				whereCol = info[2];
			}
			
//			System.out.println(name);
			tdInfoMap.put(name, tbName + "~~" + whereCol);
//			System.out.println("name: " + name + " tbName: " + tbName + " whereCol: " + whereCol);
        }
		
		return tdInfoMap;
	}
	
}
