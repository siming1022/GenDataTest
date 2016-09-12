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
            
            boolean allColRowkey = !rowkeyMap.containsKey(tableName.toUpperCase());
            if (!allColRowkey)
            	rowKey = getRowkeyStr(rowkeyMap.get(tableName));
            
            while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
        		tempSb += cols[0] + " string, \n";
        		
        		hbaseColSb += "f:" + cols[0].toUpperCase() + ","; 
              	
        		rowKey += cols[0].toUpperCase() + ":string,";
            }
            
            tempSb += "Rec_Avail_Flag string, \n";
            tempSb += "Load_Date string, \n";
            tempSb += "Load_Time string, \n";
            tempSb += "Load_Timestamp string, \n";
            
            hbaseColSb += "f:REC_AVAIL_FLAG,f:LOAD_DATE,f:LOAD_TIME,f:LOAD_TIMESTAMP,";
            
            rowKey = rowKey.substring(0, rowKey.length() - 1);
            
            tempSb = tempSb.substring(0, tempSb.length() - 3) + ") \n";
            
            hbaseColSb = hbaseColSb.substring(0, hbaseColSb.length() - 1);
            
            sb.append("key struct<" + rowKey + ">, \n");
            
            sb.append(tempSb.toString() + "");
            
            sb.append("row format delimited collection items" + "\n");
            sb.append("terminated by '~'" + "\n");
            sb.append("STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'" + "\n");
            sb.append("with SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key," + "\n");
            sb.append(hbaseColSb + "\") \n");
            sb.append("TBLPROPERTIES (\"hbase.table.name\" = \"" + tableName + "\");\n\n\n");
            
//            System.out.println("disable '" + tableName.toUpperCase() + "'");
//            System.out.println("truncate '" + tableName.toUpperCase() + "'");
//            System.out.println("truncate_preserve '" + tableName.toUpperCase() + "'");
//            System.out.println("count '" + tableName.toUpperCase() + "'");
//            System.out.println("drop '" + tableName.toUpperCase() + "'");
//            System.out.println("create '" + tableName.toUpperCase() + "', 'f'");
//            System.out.println(tableName.toUpperCase() + ".sql");
//            System.out.println("alter '" + tableName.toUpperCase() + "', {NAME => 'f', DATA_BLOCK_ENCODING => 'PREFIX', COMPRESSION => 'SNAPPY'}");
//            System.out.println("create '" + tableName.toUpperCase() + "', {NAME => 'f', DATA_BLOCK_ENCODING => 'PREFIX', COMPRESSION => 'SNAPPY', TTL => '604800'},{SPLITS => ['08bc6bc06e244c4face465a3897c16f4','10a076a862fc4fc2aa0830ecd4d38511','181bca98f8b24e5a9b7b697c43bebb63','209e9bc020d145f9976bab86dd6d97af','292ce28c09f54d7c9b4e51d19f8d12bb','32bef39aded349dfa348084fdc8d5536','3bd10a2f77844ef7865a305e9c1cd92a','445fb3e86c5c48009eab3ddecd869dd5','4ca11b641b2c4e5895c299cc416dea0d','544a1de26fa24747a69a7119effa4076','5c4106e3176f410fba973fba87ec88b9','642f60a06cb044e497c97aff6d67246b','6bbcc93fd7e048dcb700f2f1cf8794b0','7392c4fe4a4941949e355c8305704624','7d75aeae694141369026a8076bb9480f','85722097134c40769589df78fa985919','8e0ed6480a6a4fe98746b441c02bcee4','96abe0497fb54190be1158459c9bdfd4','9ef6f934dd9745d286641bdbf82da77d','a7b687da52c642dea4695505c3e2be3b','b012ef639b0d4720bd057c2b8226607e','b9fb3cd246914123a4c5875d958ba4f4','c2af7b7020444940b9ed200dd66a9901','cce98d8e26cf4ba7b4c63516f1db2b77','d5f5b104af1840c380cd039d0ef5e98d','deff7308f0684d6a8be40153bbe78bcd','e75fa6c6661347d586e532ae1db2c982','ef4a8eac235f44ca86ef8d1dbf5ce230','f7e6348291784054bde1c4e9bdf36218']}");
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
