package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class GenImportFronTd2Hdfs {

	public static void main(String[] args) throws Exception
	{
//		File dir = new File("C:\\Users\\Administrator\\Desktop\\报表\\");
		File dir = new File("D:\\bigData\\导数据");
		
		File[] fs = dir.listFiles();
		
			for (File f : fs)
			{
		        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
		    		BufferedReader br = new BufferedReader(isr);
		    		String line = "";
		        		 
		            String tableName = f.getName().replaceAll(".txt", "").replaceAll(".TXT", "");
		            
		            
		            StringBuffer sb = new StringBuffer(" sqoop import --driver com.ncr.teradata.TeraDriver  --connect jdbc:teradata://10.3.10.50/TSNANO=0,CLIENT_CHARSET=cp936,TMODE=TERA,CHARSET=ASCII,DATABASE=EMS_PVIEW --username udev --password udev_ems --query \"");
		            sb.append("select ");
		            String tempStr = "";
		            
		            while ((line = br.readLine()) != null) 
		            {
		            		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
		            		
		            		String colName = cols[0].toUpperCase();
		            		String type = cols[1].toUpperCase();
		            		if ("LOAD_TIME".equals(colName))
		            		{
		                		tempStr += "cast(" + colName + " as varchar(8)) " + " as " + colName + ", ";        			
		            		}
		            		else if ("LOAD_DATE".equals(colName))
		            		{
		            			tempStr += "CAST((" + colName + " (FORMAT 'YYYYMMDD')) AS VARCHAR(26)) " + " as " + colName + ", ";       
		            		}
		            		else if ("LOAD_TIMESTAMP".equals(colName))
		            		{
		            			tempStr += "CAST((" + colName + " (FORMAT 'YYYY-MM-DDBHH:MI:SS')) AS VARCHAR(26)) " + " as " + colName + ", ";       
		            		}
		            		else if (type.contains("INTEGER") || type.contains("DECIMAL"))
		            		{
		            			tempStr += "CAST(" + colName + "  AS VARCHAR(26)) " + " as " + colName + ", ";      
		            		}
		            		else if (type.contains("DATE"))
		            		{
		            			tempStr += "CAST((" + colName + " (FORMAT 'YYYYMMDD')) AS VARCHAR(26)) " + " as " + colName + ", ";   
		            		}
		            		else
		            		{
		            			tempStr += "cast(" + colName + " as varchar(500)) " + " as " + colName + ", ";        	
		            		}
		            }
		            
		            String whereCols = "";
		            
		            
//		            to_char(sysdate, 'YYYYMMDD') as load_date,to_char(sysdate, 'HHMMSS') as load_time, sysdate as load_timestamp
		            
		            sb.append(tempStr.substring(0, tempStr.length() - 2));
		            sb.append(" from EMS_PMART." + tableName.toUpperCase() + " where " + whereCols + " \\$CONDITIONS ");
		            sb.append("\" --append --target-dir /EMS_Data/teamsun/" + tableName.toUpperCase() + " --null-string '\\\\N' --null-non-string '\\\\N'  --m 1");
		            
		            
		            System.out.println(sb.toString());
		            System.out.println();
//			            writeToFile(tableName, sb.toString());
//			            System.out.println("hdfs dfs -rmr /EMS_Data/teamsun/" + tableName.toUpperCase() + " ");
//		            System.out.println("insert into ems_pmart." + tableName.toUpperCase() + " SELECT * FROM test." + tableName.toUpperCase());
		            
		        }
	}
	
	private static void writeToFile(String tableName, String string) 
	{
		try 
		{
			File f = new File("C:\\Users\\Administrator\\Desktop\\历史数据迁移\\" + tableName + ".txt");
			PrintWriter wr = new PrintWriter(f);
			wr.println(new String(string.getBytes("UTF-8")));
			wr.close();
		} 
		catch (Exception e) 
		{
		}
	}

	private static String changeType(String type) 
	{
		if (type.contains("VARCHAR") || type.contains("CHAR") || type.contains("DATE"))
		{
			return "string";
		}
		else if (type.contains("INTEGER"))
		{
			return "int";
		}
		else if (type.contains("DECIMAL"))
		{
			return "double";
		}
		else
		{
			return "string";
		}
	}

}
