package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;


/**  
 * 读取数据库表结构文件生成protobuf数据
 * 
 * @Title: Test12.java
 * @Description: TODO
 * @author: Administrator
 * @date: 2015年6月3日 下午6:02:44
 */
public class GenProtobufByDataBase 
{

	public static void main(String args[]) {
		try 
		{
			File dir = new File("D:\\bigData\\导数据\\");
			 File[] fs = dir.listFiles();
	         
	         for (File f : fs)
	         {
	        	 InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
	        	 BufferedReader br = new BufferedReader(isr);
	        	 String line = "";
	        	 
	        	 String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
	        	 String family = "f";
	        	 String entityName = "EMS_Field_List.FeedbackMid";
	        	 String v = "msg.getFeedbackMid().get";
	        	 
//	        	 genProtobuf(f);
//	        	 genJavaStatic(f);
	        	 genHbaseInput(f);
	        	 
	        	 int i = 0;
	        	 
//	        	 StringBuffer sb = new StringBuffer(tableName + ".put(new Put(rowkey.getBytes())");
	        	 
	        	 while ((line = br.readLine()) != null) 
	        	 {
	        		 String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
	        		 
//				System.out.println("public static final byte[] " + colName + " = \"" + colName +"\".getBytes();");
	        		 
//	        		 sb.append(".add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(" + v + trans(colName) + "()))" + "\n");
	        	 }
	        	 
//	        	 System.out.println(sb.toString());
	         }
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	private static void genHbaseInput(File f) throws Exception
	{
		InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
	   	 BufferedReader br = new BufferedReader(isr);
	   	 String line = "";
		 String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
	   	
		 String family = "f";
    	 String entityName = "EMS_Field_List." + tableName;
    	 String v = "msg.get" + tableName + "().get";
    	 
	   	 StringBuffer sb = new StringBuffer(f.getName().toUpperCase().replaceAll(".TXT", "") + ".put(new Put(rowkey.getBytes())");
	   	 
	   	 while ((line = br.readLine()) != null) 
	   	 {
	   		String colName = line.substring(line.indexOf("      ")+6).split(" ")[0].toUpperCase();
	   		sb.append(".add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(" + v + tranCol(colName) + "()))" + "\n");
	   	 }
	   	 
	   	 
	   	 sb.append(" ); \n ");
	   	 System.out.println(sb.toString());
	}



	private static void genJavaStatic(File f)  throws Exception
	{
		InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
	   	 BufferedReader br = new BufferedReader(isr);
	   	 String line = "";
		 String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
	   	
	   	 StringBuffer sb = new StringBuffer("public static class " + tableName + " { \n");
	   	 
	   	 while ((line = br.readLine()) != null) 
	   	 {
	   		String colName = line.substring(line.indexOf("      ")+6).split(" ")[0].toUpperCase();
	   		sb.append("	public static final byte[] " + colName + " = \"" + colName +"\".getBytes(); \n");
	   	 }
	   	 
	   	sb.append("public static final byte[] Rec_Avail_Flag = \"REC_AVAIL_FLAG\".getBytes(); \n");
	   	sb.append("public static final byte[] Load_Date = \"LOAD_DATE\".getBytes(); \n");
	   	sb.append("public static final byte[] Load_Time = \"LOAD_TIME\".getBytes(); \n");
	   	sb.append("public static final byte[] Load_TimeStamp = \"LOAD_TIMESTAMP\".getBytes(); \n");
	   	 
	   	 sb.append("} \n");
	   	 sb.append("public static class " + tableName + "_List{} \n\n");
	   	 
	   	 System.out.println(sb.toString());
	}


	private static void genProtobuf(File f) throws Exception
	{
		 InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
	   	 BufferedReader br = new BufferedReader(isr);
	   	 String line = "";
		 String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
	   	 int i = 0;
	   	 
	   	 StringBuffer sb = new StringBuffer("message " + tableName + " { \n");
	   	 
	   	 while ((line = br.readLine()) != null) 
	   	 {
	   		 String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
	   		 sb.append("optional string " + cols[0] + " = " + (++i) + "; \n");
	   	 }
	   	 
	   	 sb.append(" } \n");
	   	 
	   	 sb.append("message " + tableName + "_LIST { \n");
	   	 sb.append("	repeated " + tableName + " " + tableName.toLowerCase() + "list = 1; \n");
	   	 sb.append("} \n");
	   	 
	   	 System.out.println(sb.toString());
	}
	
	/**
	 * 
	 * @Title: Test13
	 * @Description: TODO
	 * @author: Administrator
	 * @param colName
	 * @return
	 * @throws: 
	 */
	private static String tranCol(String colName) 
	{
		String res = "";
		if (colName.indexOf("_") != -1)
		{
			String[] tempStrs = colName.split("_");
			res = trans(tempStrs);
		}
		else
		{
			res = colName.substring(0, 1).toUpperCase() + colName.substring(1, colName.length()).toLowerCase();
		}
		
		return res;
	}
	
	
	/**
	 * 将def文件名转换成JAVA的驼峰命名
	 * 
	 * @Title: Test13
	 * @Description: TODO
	 * @author: Administrator
	 * @param fileName
	 * @return
	 * @throws: 
	 */
	private static String transTableName(String fileName) 
	{
		String res = "";
		if (fileName.indexOf("_") != -1)
		{
			String[] tempStrs = fileName.split("_");

			if (tempStrs.length == 3)
			{
				String[] dest = new String[2];
				System.arraycopy(tempStrs, 1, dest, 0, 2);
				res = trans(dest);
			}
			else
			{
				String[] dest = new String[tempStrs.length - 2];
				System.arraycopy(tempStrs, 2, dest, 0, tempStrs.length - 2);
				res = trans(dest);
			}
		}
		
		return res;
	}
	
	private static String trans(String[] tempStrs) 
	{
		String res = "";
		
		for (int i = 0; i < tempStrs.length; i++)
		{
			res += (tempStrs[i].substring(0, 1).toUpperCase() + tempStrs[i].substring(1, tempStrs[i].length()).toLowerCase());
		}
		
		return res;
	}
}
