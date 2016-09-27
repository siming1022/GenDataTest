package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Calendar;

public class HdfsUtil 
{
	public static void main(String[] args) throws Exception
	{
		File dir = new File("D:\\bigData\\导数据");
		File[] fs = dir.listFiles();
		
		for (File f : fs)
		{
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
    		StringBuffer queryCols = new StringBuffer();
    		String tableName = f.getName().toUpperCase().replaceAll(".TXT", "");
    		while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
        		queryCols.append("NVL(" + cols[0] + ", '') AS " + cols[0] + ",");
            }
    		
    		queryCols = new StringBuffer(queryCols.substring(0, queryCols.length() - 1));
    		
    		Calendar c = Calendar.getInstance();
    		c.set(Calendar.YEAR, 2016);
    		c.set(Calendar.MONTH, 04);
    		c.set(Calendar.DAY_OF_MONTH, 07);
    		
    		Calendar c2 = Calendar.getInstance();
    		c2.set(Calendar.YEAR, 2016);
    		c2.set(Calendar.MONTH, 7);
    		c2.set(Calendar.DAY_OF_MONTH, 16);
    		
    		while (c2.after(c))
    		{
//    			String command = "hdfs dfs -mkdir /EMS_Data/teamsun/" + tableName + "/" + c.get(Calendar.YEAR) + "" + ((c.get(Calendar.MONTH) + 1) < 10?"0"+(c.get(Calendar.MONTH) + 1):(c.get(Calendar.MONTH) + 1)) + "" +  ((c.get(Calendar.DAY_OF_MONTH)<10?"0"+c.get(Calendar.DAY_OF_MONTH):c.get(Calendar.DAY_OF_MONTH)));
    			String date = c.get(Calendar.YEAR) + "" + ((c.get(Calendar.MONTH) + 1) < 10?"0"+(c.get(Calendar.MONTH) + 1):(c.get(Calendar.MONTH) + 1)) + "" +  ((c.get(Calendar.DAY_OF_MONTH)<10?"0"+c.get(Calendar.DAY_OF_MONTH):c.get(Calendar.DAY_OF_MONTH)));
    			String hdfsLoc = "/EMS_Data/teamsun/" + tableName + "/" + date;
    			
//    			String command = "hdfs dfs -chmod 777 /EMS_Data/teamsun/" + tableName + "/" + date;
//    			String command = "INSERT OVERWRITE DIRECTORY '" + hdfsLoc + "' row format delimited fields terminated by '\\t' SELECT " + queryCols + " FROM EMS_PDATA_RANGE." + tableName + " WHERE DLV_DATE = '" + date + "';";
    			
    			String command = "hdfs dfs -getmerge /EMS_Data/teamsun/" + tableName + "/" + date + "/* $DIR/part-m-00000\r\n";
    			command += "hdfs dfs -rm /EMS_Data/teamsun/" + tableName + "/" + date + "/*\r\n";
    			command += "hdfs dfs -put $DIR/part-m-00000 /EMS_Data/teamsun/" + tableName + "/" + date + "/\r\n";
    			command += "rm -f $DIR/part-m-00000\r\n\r\n";
//    			hdfs dfs -getmerge /EMS_Data/teamsun/TB_EVT_DLV/20160814/* ./part-m-00000
//    			hdfs dfs -rm /EMS_Data/teamsun/TB_EVT_DLV/20160814/*
//    			hdfs dfs -put part-m-00000 /EMS_Data/teamsun/TB_EVT_DLV/20160814/
//    			rm -f ./part-m-00000
    			System.out.println(command); 
//    			writeToFile(tableName, command);
    			c.add(Calendar.DAY_OF_YEAR, 1);
    		}
		}
	}
	
	private static void writeToFile(String tableName, String string) 
	{
		try 
		{
			File f = new File("C:\\Users\\Administrator\\Desktop\\" + tableName + ".txt");
			FileWriter wr = new FileWriter(f, true);
			wr.write(new String(string.getBytes("UTF-8")) + "\r\n");
			wr.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
}
