package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenMergeSql {

	public static void main(String[] args) throws Exception
	{
		File dir = new File("D://bigData//orcMerge/");
		
		File[] fs = dir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) 
			{
				if (name.contains(".TXT") || name.contains(".txt"))
					return true;
				
				return false;
			}
		});
		
		Map<String, String> clusterMap = getClustereds();
		
        for (File f : fs)
        {
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
        		 
            String tableName = f.getName().replace(".txt", "").replace(".TXT", "").toUpperCase();
            String tempTableName = tableName + "_TEMP";
//            System.out.println(tableName);
            StringBuffer sb = new StringBuffer("set transaction.type = inceptor; \n");
            sb.append("drop table if exists " + tempTableName + "; \n");
            sb.append("create temporary table " + tempTableName + " stored as orc as select * from ems_pdata." + tableName + " where load_date='qqyyrr'; \n");
            sb.append("MERGE INTO ems_pdata_orc." + tableName + " PARTITION (yearmonth = 'qqyymm') a USING " + tempTableName + " b ON (a.key = b.key) WHEN MATCHED THEN UPDATE SET ");
            
            String aSql = "";
            String bSql = "";
            String abSql = "";
            
            String noCol = clusterMap.get(tableName).split("-")[0].toLowerCase();
            while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
        		aSql += "a." + cols[0] + ",";
        		bSql += "b." + cols[0] + ",";
        		if (!cols[0].toLowerCase().equals(noCol))
        		{
        			abSql += "a." + cols[0] + " = b." + cols[0] + ",";
        		}
            }
            
            aSql = aSql.substring(0, aSql.length() - 1);
            bSql = bSql.substring(0, bSql.length() - 1);
            abSql = abSql.substring(0, abSql.length() - 1);
            
            sb.append(abSql + " WHEN NOT MATCHED THEN INSERT (");
            sb.append(aSql + ") VALUES (");
            sb.append(bSql + "); ");
            
//            System.out.println(sb.toString() + "\n\n\n");
            writeToFile(tableName, sb.toString());
            
            
        }
	}
	
	private static void writeToFile(String tableName, String string) 
	{
		try 
		{
			File f = new File("d:/bigData/orcMerge/sql/" + tableName + ".sql");
			PrintWriter wr = new PrintWriter(f);
			wr.println(new String(string.getBytes("UTF-8")));
			wr.close();
		} 
		catch (Exception e) 
		{
		}
	}
	
	private static Map<String, String> getClustereds() throws Exception
	{
		Map<String, String> clusteredMap = new HashMap<String, String>();
		File rowkeyFile = new File("D:\\bigData\\clusteredInfo.txt");
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(rowkeyFile), "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		while ((line = br.readLine()) != null) 
		{
//			System.out.println(line);
			String tbName = line.split("\\t")[0];
			String clusters = line.split("\\t")[1];
			String num = line.split("\\t")[2];
			
			
			clusteredMap.put(tbName, clusters + "-" + num);
		}
		
		return clusteredMap;
	}
}
