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

public class GenCreateHbaseHiveTable {

	public static void main(String[] args) throws Exception
	{
		File dir = new File("D://bigData//导数据/");
		
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
		
        
        for (File f : fs)
        {
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
    		int i = 0;
        		 
        	//生成hive 外表建表语句
            String tableName = f.getName().replace(".txt", "").replace(".TXT", "").toUpperCase();
//            System.out.println(tableName);
            String rowKey = "";
            StringBuffer sb = new StringBuffer("  drop table if exists ems_pdata." + tableName + "; \n");
            
            sb.append(" create EXTERNAL table ems_pdata." + tableName + "(");
            
            String tempSb = "";
            String hbaseColSb = "";

            while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
        		tempSb += cols[0] + " string, \n";
        		
        		hbaseColSb += "f:" + cols[0].toUpperCase() + ","; 
              	
            }
            
            
            tempSb = tempSb.substring(0, tempSb.length() - 3) + ") \n";
            rowKey = getRowkeyStr(rowkeyMap.get(tableName));
            hbaseColSb = hbaseColSb.substring(0, hbaseColSb.length() - 1);
            
            sb.append("key struct<" + rowKey + ">, \n");
            
            sb.append(tempSb.toString() + "\n");
            
            sb.append("row format delimited collection items" + "\n");
            sb.append("terminated by '~'" + "\n");
            sb.append("STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'" + "\n");
            sb.append("with SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key," + "\n");
            sb.append(hbaseColSb + "\") \n");
            sb.append("TBLPROPERTIES (\"hbase.table.name\" = \"" + tableName + "\");");
            
            
//            System.out.println("alter '" + tableName + "', {NAME => 'f', DATA_BLOCK_ENCODING => 'PREFIX', COMPRESSION => 'SNAPPY'}");
//            System.out.println("-----------------------------------" + tableName + "------------------------------------------");
            System.out.println(sb.toString());
            
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

}
