package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GenImportFromTb2HbaseSql {

	public static void main(String[] args) throws Exception
	{
		File dir = new File("D://bigData//导数据/");
		
		File[] fs = dir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if (name.contains("txt") || name.contains("TXT"))
					return true;
				else
					return false;
			}
		});
		
		Map<String, List<String>> rowkeys = getRowkeys();
		Map<String, String> tdTableInfos = getTdTableInfo();
        
        for (File f : fs)
        {
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "UTF-8");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
    		int i = 0;
        		 
            String tableName = f.getName().replaceAll(".txt", "").replaceAll(".TXT", "");
            
            //sqoop import --driver com.ncr.teradata.TeraDriver  --connect jdbc:teradata://10.3.10.50/TSNANO=0,CLIENT_CHARSET=cp936,TMODE=TERA,CHARSET=ASCII,DATABASE=EMS_PVIEW --username udev --password udev_ems --query "select Mway_Code as Mway_Code, Flight_Num as Flight_Num, Filght_Flag as Filght_Flag, CAST((start_date (FORMAT 'YYYYMMDD')) AS VARCHAR(26)) as Start_Date, CAST((End_Date (FORMAT 'YYYYMMDD')) AS VARCHAR(26)) as End_Date, REC_AVAIL_Flag as REC_AVAIL_Flag, CAST((Load_Date (FORMAT 'YYYYMMDD')) AS VARCHAR(26)) as Load_Date, cast(Load_Time as varchar(8)) as Load_Time, CAST((load_timestamp (FORMAT 'YYYY-MM-DDBHH:MI:SS')) AS VARCHAR(26)) as Load_TimeStamp, Mway_Code || '~' || Flight_Num  || '~' ||  Filght_Flag as ROWKEY from EMS_PVIEW.VW_RES_FLIGHT_ROUTE_RELA where \$CONDITIONS "  --hbase-table TB_TEST --column-family f   --hive-drop-import-delims --null-string '\\N' --null-non-string '\\N'   --hbase-row-key ROWKEY  --m 1 

            StringBuffer sb = new StringBuffer("sqoop import --driver com.ncr.teradata.TeraDriver  --connect jdbc:teradata://10.3.10.50/TSNANO=0,CLIENT_CHARSET=cp936,TMODE=TERA,CHARSET=ASCII,DATABASE=EMS_PVIEW --username udev --password udev_ems --query \"");
            
            sb.append("select ");
            String tempStr = "";
            
            String rowkeyCols = getRowkey(tableName, rowkeys);
            boolean isAllTable = rowkeyCols==null?true:false;
            
            if (isAllTable)
            {
            	rowkeyCols = "";
            }
            
            while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
        		String colName = cols[0].toUpperCase();
        		String type = cols[1].toUpperCase();
        		if (type.startsWith("INTEGER") || type.startsWith("DECIMAL") || type.startsWith("CHAR"))
        		{
        			tempStr += "CAST(" + colName + "  AS VARCHAR(26)) " + " as " + colName + ", ";
        			if (isAllTable)
        			{
        				rowkeyCols += "COALESCE(CAST(" + colName + "  AS VARCHAR(26)), '" + colName + "_NULL') || '~' || ";
        			}
        		}
        		else if (type.startsWith("DATE"))
        		{
        			tempStr += "CAST((" + colName + " (FORMAT 'YYYYMMDD')) AS VARCHAR(26)) " + " as " + colName + ", "; 
        			if (isAllTable)
        			{
        				rowkeyCols += "COALESCE(CAST((" + colName + " (FORMAT 'YYYYMMDD')) AS VARCHAR(26)), '" + colName + "_NULL') || '~' || ";
        			}
        		}
        		else if (type.startsWith("TIMESTAMP"))
        		{
        			tempStr += colName + " as " + colName + ", ";      
        			if (isAllTable)
        			{
        				rowkeyCols += "COALESCE(CAST((" + colName + " (FORMAT 'YYYYMMDD')) AS VARCHAR(26)), '" + colName + "_NULL') || '~' || ";
        			}
        		}
        		else
        		{
        			tempStr += colName + " as " + colName + ", ";      
            		if (isAllTable)
        			{
        				rowkeyCols += "COALESCE(" + colName + ", '" + colName  + "_NULL') || '~' || ";
        			}
        		}


            }
            
            if (isAllTable)
            {
            	rowkeyCols = rowkeyCols.substring(0, rowkeyCols.length() - 11) + " AS ROWKEY ";
            }
            
//            System.out.println(tableName);
            String[] infos = tdTableInfos.get(tableName).split("~~");
            String tdTableName = infos[0];
            String whereCols = "";
            
            if(infos.length > 1)
            {
            	for (String whereCol : infos[1].split(","))
            	{
            		whereCols += whereCol + " and " ;
            		
            	}
            }
            
            sb.append(tempStr.substring(0, tempStr.length() - 2) + ", " +rowkeyCols);
            sb.append(" from " + tdTableName + " where " + whereCols + " \\$CONDITIONS ");
            
            sb.append("\"   --hbase-table " + tableName + " --column-family f  --hive-drop-import-delims --null-string '\\\\N' --null-non-string '\\\\N'   --hbase-row-key ROWKEY  --m 1");
            
//            writeToFile(tableName, sb.toString());
            System.out.println(sb.toString());
//            System.out.println("truncate '" + tableName.toUpperCase() + "'");
//            System.out.println(sb.toString());
//            System.out.println("create table ems_pdata.TMP_" + tableName.toUpperCase() + " as select * from ems_pdata." + tableName.toUpperCase() + ";");
            
            
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

	private static String getRowkey(String tableName, Map<String, List<String>> rowkeys) throws Exception
	{
		if (!rowkeys.containsKey(tableName))
		{
			return null;
		}
		
		List<String> ll = rowkeys.get(tableName);
		
		if (ll == null || ll.size() == 0)
		{
			return null;
		}
		
		String rk = "";
		
		for (String s : ll)
		{
			rk += s + " || '~' || ";
		}
		
		return rk.substring(0, rk.length() - 11) + " as ROWKEY ";
	}
	
	private static Map<String, List<String>> getRowkeys() throws Exception
	{
		Map<String, List<String>> rowkeyMap = new LinkedHashMap<String, List<String>>();
		File rowkeyFile = new File("D:\\bigData\\rowkeys.txt");
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(rowkeyFile), "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		while ((line = br.readLine()) != null) 
        {
//			System.out.println(line);
			String tbName = line.split("\\t")[0];
			String[] rowkeys = line.split("\\t")[1].split(",");
			
			List<String> ll = new ArrayList<String>();

			for (String rowkey : rowkeys)
			{
				ll.add(rowkey.toUpperCase());
			}
			
			rowkeyMap.put(tbName, ll);
        }
		
		return rowkeyMap;
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
