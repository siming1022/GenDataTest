package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class GenTdExportSql {

	public static void main(String[] args) throws Exception
	{
		File dir = new File("D:\\bigData\\导数据");
		
		File[] fs = dir.listFiles();
		
		for (File f : fs)
		{
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
    		int i = 0;
        		 
            String tableName = f.getName().replaceAll(".txt", "").replaceAll(".TXT", "");
            
            

            StringBuffer sb = new StringBuffer("SELECT \n");
            
            while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
//        		sb.append("Trim(coalesce(" + cols[0] + ",''))" + " ||','|| \n");
//        		sb.append("Trim(" + cols[0] + ")" + " ||','|| \n");
//        		sb.append("cast("cols[0] + "," + " ||','|| \n");
        		String colName = cols[0];
        		String type = cols[1].toUpperCase();
        		if (type.contains("INTEGER") || type.contains("DECIMAL"))
        		{
        			sb.append( "CAST(" + colName + "  AS VARCHAR(26)) " + " as " + colName + ", \n");      
        		}
        		else if (type.contains("DATE"))
        		{
        			sb.append("CAST((" + colName + " (FORMAT 'YYYYMMDD')) AS VARCHAR(26)) " + " as " + colName + ", \n");   
        		}
        		else
        		{
        			sb.append("cast(" + colName + " as varchar(500)) " + " as " + colName + ", \n");        	
        		}
        		
            }
            
            
            sb = new StringBuffer(sb.substring(0, sb.length() - 3));
            sb.append("\n FROM  HIS_PDATA." + tableName + "\n");
            
            
            System.out.println(sb.toString());
            
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
