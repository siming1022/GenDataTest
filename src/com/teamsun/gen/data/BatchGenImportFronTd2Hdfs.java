package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class BatchGenImportFronTd2Hdfs {

	public static void main(String[] args) throws Exception
	{
		File dir = new File("D:\\bigData\\导数据");
		
		File[] fs = dir.listFiles();
		Map<String, String> tdTableInfos = getTdTableInfo();
			for (File f : fs)
			{
		        		 
		    		Calendar c = Calendar.getInstance();
		    		c.set(Calendar.YEAR, 2015);
		    		c.set(Calendar.MONTH, 00);
		    		c.set(Calendar.DAY_OF_MONTH, 01);
		    		
		    		Calendar c2 = Calendar.getInstance();
		    		c2.set(Calendar.YEAR, 2016);
		    		c2.set(Calendar.MONTH, 4);
		    		c2.set(Calendar.DAY_OF_MONTH, 07);
		    		
		    		while (c2.after(c))
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
//			            			tempStr += "cast(ems_pdata.mreplace(" + colName + ", '0a'xc||'# ', '#') as varchar(500)) " + " as " + colName + ", ";        	
			            			tempStr += "cast(ems_pdata.mreplace(" + colName + ",  '0a'xc||'#'||' #'||'0d'xc||'# ', '#') as varchar(500)) " + " as " + colName + ", ";        	
//			            			tempStr += "cast(" + colName + " as varchar(500)) " + " as " + colName + ", ";        	
			            		}
			            }
			            
		            	String whereCols = "";
//			            System.out.println(tableName);
			            String[] infos = tdTableInfos.get(tableName).split("~~");
			            String tdTableName = infos[0];
			            String dateCol = null;
			            String dateType = null;
			            
			            if (infos.length > 1)
			            {
			            	dateCol = infos[1];
			            }
			            else
			            {
//			            	System.out.println(tableName);
			            	break;
			            }
			            
			            if (infos.length > 2)
			            {
			            	dateType = infos[2];
			            }
			            
	//		            to_char(sysdate, 'YYYYMMDD') as load_date,to_char(sysdate, 'HHMMSS') as load_time, sysdate as load_timestamp
			            
			            	sb.append(tempStr.substring(0, tempStr.length() - 2));
			            
			            	if (dateType != null && "b".equals(dateType.toLowerCase()))
			            	{
			            		whereCols = c.get(Calendar.YEAR) + "-" + ((c.get(Calendar.MONTH) + 1) < 10?"0"+(c.get(Calendar.MONTH) + 1):(c.get(Calendar.MONTH) + 1)) + "-" +  ((c.get(Calendar.DAY_OF_MONTH)<10?"0"+c.get(Calendar.DAY_OF_MONTH):c.get(Calendar.DAY_OF_MONTH)));
			            	}
			            	else
			            	{
			            		whereCols = c.get(Calendar.YEAR) + "" + ((c.get(Calendar.MONTH) + 1) < 10?"0"+(c.get(Calendar.MONTH) + 1):(c.get(Calendar.MONTH) + 1)) + "" +  ((c.get(Calendar.DAY_OF_MONTH)<10?"0"+c.get(Calendar.DAY_OF_MONTH):c.get(Calendar.DAY_OF_MONTH)));
			            	}
	//		    			System.out.println((c.get(Calendar.MONTH) + 1) + ":"+ c.get(Calendar.DAY_OF_MONTH));
			    			sb.append(" from " + tdTableName + " where " + dateCol + " = '" + whereCols + "' and \\$CONDITIONS ");
			    			sb.append("\" --append --target-dir /EMS_Data/teamsun/" + tableName.toUpperCase() + "/" + whereCols.replaceAll("-", "") + " --null-string '' --null-non-string '' --fields-terminated-by '\\t' --m 1");
			    			c.add(Calendar.DAY_OF_YEAR, 1);
	//		    			System.out.println(whereCols);
//			    			System.out.println(sb.toString());
			    			writeToFile(tableName.toUpperCase(), sb.toString());
		    		}
//		    		System.out.println("end");
		    		
		            
		            
//			            writeToFile(tableName, sb.toString());
//			            System.out.println("hdfs dfs -rmr /EMS_Data/teamsun/" + tableName.toUpperCase() + " ");
		            
		            
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
			String dateType = "";
			
			if (info.length > 2)
			{
				whereCol = info[2];
			}

			if (info.length > 3)
			{
				dateType = info[3];
			}
			
//			System.out.println(name);
			tdInfoMap.put(name, tbName + "~~" + whereCol + "~~" + dateType);
//			System.out.println("name: " + name + " tbName: " + tbName + " whereCol: " + whereCol);
        }
		
		return tdInfoMap;
	}
	
	private static void writeToFile(String tableName, String string) 
	{
		try 
		{
			File f = new File("C:\\Users\\Administrator\\Desktop\\TD历史数据备份\\未导\\" + tableName + ".txt");
			FileWriter wr = new FileWriter(f, true);
			wr.write(new String(string.getBytes("UTF-8")) + "\r\n");
			wr.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
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
