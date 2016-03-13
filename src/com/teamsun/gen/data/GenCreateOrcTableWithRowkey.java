package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenCreateOrcTableWithRowkey {

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
		
		Map<String, List<String>> rowkeyMap = new HashMap<String, List<String>>();
		rowkeyMap = getRowkeys();
		Map<String, String> clusterMap = getClustereds();
        for (File f : fs)
        {
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
    		int i = 0;
        		 
        	//生成hive 外表建表语句
            String tableName = f.getName().replace(".txt", "").replace(".TXT", "").toUpperCase();
//            System.out.println(tableName);
            StringBuffer sb = new StringBuffer("  drop table if exists ems_pdata_orc." + tableName + "; \n");
            
            sb.append(" create table ems_pdata_orc." + tableName + "( \n");
            
            String rowkey = getRowkeyStr(rowkeyMap.get(tableName));
            String tempSb = "  key struct<" + rowkey + ">, \n";
            while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
        		tempSb += cols[0] + " " + changeType(cols[1]) + ", \n";
        		
            }
            
            tempSb = tempSb.substring(0, tempSb.length() - 3) + ") \n";
            String[] clusterInfo = clusterMap.get(tableName).split("-");
            
            sb.append(tempSb.toString());
            sb.append("PARTITIONED BY (yearmonth string) \n" );
            sb.append("CLUSTERED BY (" + clusterInfo[0] + ") INTO " + clusterInfo[1] + " BUCKETS \n" );
            sb.append("STORED AS ORC \n" );
            sb.append("TBLPROPERTIES (\"transactional\"=\"true\"); \n\n" );
            
//            sb.append("ROW FORMAT DELIMITED  " + "\n");
//            sb.append("FIELDS TERMINATED BY \'\\,\'" + "\n");
//            sb.append("STORED AS INPUTFORMAT " + "\n");
//            sb.append("'org.apache.hadoop.mapred.TextInputFormat'" + "\n");
//            sb.append("OUTPUTFORMAT" + "\n");
//            sb.append("'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \n");
//            sb.append("LOCATION \n");
//            sb.append("'/EMS_Data/teamsun/" + tableName.toUpperCase() + "'; \n");
            
//            System.out.println("alter '" + tableName + "', {NAME => 'f', DATA_BLOCK_ENCODING => 'PREFIX', COMPRESSION => 'SNAPPY'}");
//            System.out.println("-----------------------------------" + tableName + "------------------------------------------");
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
	
	private static String getRowkeyStr(List<String> list) 
	{
		String rowkeyStr = "";
//		System.out.println(list);
		for (String rk : list)
		{
			rowkeyStr += rk + ":string,";
		}
		return rowkeyStr.substring(0, rowkeyStr.length() - 1);
	}

	private static Map<String, List<String>> getRowkeys() throws Exception
	{
		Map<String, List<String>> rowkeyMap = new HashMap<String, List<String>>();
		File rowkeyFile = new File("D:\\bigData\\rowkeys.txt");
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(rowkeyFile), "GBK");
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
