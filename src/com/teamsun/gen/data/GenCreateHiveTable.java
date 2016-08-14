package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class GenCreateHiveTable {

	public static void main(String[] args) throws Exception
	{
		File dir = new File("D:\\bigData\\导数据");
		
		File[] fs = dir.listFiles();
		Map<String, String> clusterMap = getClustereds();
		
		for (File f : fs)
		{
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
    		int i = 0;
        		 
        	//生成hive 外表建表语句
            String tableName = f.getName().replaceAll(".txt", "").replaceAll(".TXT", "");
            
//			            StringBuffer sb = new StringBuffer("");
            
//			            StringBuffer sb = new StringBuffer(" drop table if exists ems_pmart.TMP_" + tableName + "; \n");
//			            sb.append("create EXTERNAL table ems_pmart.TMP_" + tableName + "(");

            StringBuffer sb = new StringBuffer(" drop table if exists ems_pdata_range." + tableName + "; \n");
            sb.append("create table ems_pdata_range." + tableName + "(");
            StringBuffer queryCols = new StringBuffer();
            String clustCol = "";
            boolean clustFlag = false;
            while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
        		sb.append(cols[0] + " " + changeType(cols[1]) + "," + " \n");
        		
        		queryCols.append(cols[0] + " AS " + cols[0] + ",");
        		
        		
        		if (!clustFlag)
        		{
        			if (cols[0].toLowerCase().contains("custcd"))
        			{
        				clustCol = cols[0];
        				clustFlag = true;
        			}
        			else if (cols[0].toLowerCase().contains("cporgcd"))
        			{
        				clustCol = cols[0];
        				clustFlag = true;
        			}
        			else if (cols[0].toLowerCase().contains("postnbr"))
        			{
        				clustCol = cols[0];
        				clustFlag = true;
        			}
        		}
            }
            
            queryCols = new StringBuffer(queryCols.toString().substring(0, queryCols.length() - 1));
            sb = new StringBuffer(sb.substring(0, sb.length() - 3));
            sb.append(") " + "\n");
            
            /**
             * 建Orc外表
             *
             **/
            
            /*sb.append("PARTITIONED BY (yearmonth string) \n" );
            sb.append("CLUSTERED BY (" + "" + ") INTO " + 2 + " BUCKETS \n" );
            sb.append("STORED AS ORC \n" );
            sb.append("TBLPROPERTIES (\"transactional\"=\"true\"); \n\n" );*/
            
            sb.append("PARTITIONED BY RANGE (DataDt string) ( \n" );
            Calendar c = Calendar.getInstance();
    		c.set(Calendar.YEAR, 2015);
    		c.set(Calendar.MONTH, 00);
    		c.set(Calendar.DAY_OF_MONTH, 01);
    		
    		Calendar c2 = Calendar.getInstance();
    		c2.set(Calendar.YEAR, 2018);
    		c2.set(Calendar.MONTH, 11);
    		c2.set(Calendar.DAY_OF_MONTH, 01);
    		
    		while (c2.after(c))
    		{
    			c.add(Calendar.MONTH, 1);
    			sb.append(" PARTITION VALUES LESS THAN ('" + c.get(Calendar.YEAR) + "" + ((c.get(Calendar.MONTH) + 1) < 10?"0"+(c.get(Calendar.MONTH) + 1):(c.get(Calendar.MONTH) + 1)) + "'), \n");
    		}
            
    		sb.append(" PARTITION VALUES LESS THAN (MAXVALUE) \n");
    		sb.append(") \n");
//            String[] clusterInfo = clusterMap.get(tableName).split("-");
    		
			sb.append("CLUSTERED BY (" + clustCol + ") INTO " + 7 + " BUCKETS \n" );
    		
            sb.append("STORED AS ORC \n" );
            sb.append("TBLPROPERTIES (\"transactional\"=\"true\"); \n\n" );
            
            /*sb.append("ROW FORMAT SERDE \n");
            sb.append(" 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'  \n");
            sb.append("STORED AS INPUTFORMAT \n");
            sb.append("'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' \n");
            sb.append("OUTPUTFORMAT \n");
            sb.append("'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'; \n");
            
            /**
             * 建文件文件外表
             *
             **/
            /*sb.append("ROW FORMAT DELIMITED  " + "\n");
            sb.append("FIELDS TERMINATED BY \'\\t\'" + "\n");
            sb.append("STORED AS INPUTFORMAT " + "\n");
            sb.append("'org.apache.hadoop.mapred.TextInputFormat'" + "\n");
            sb.append("OUTPUTFORMAT" + "\n");
            sb.append("'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \n");
            sb.append("LOCATION \n");
            sb.append("'/EMS_Data/teamsun/temp/" + tableName.toUpperCase() + "'; \n");*/
            
            /*sb.append("ROW FORMAT DELIMITED  " + "\n");
            sb.append("FIELDS TERMINATED BY \'\\t\'" + "\n");
            sb.append("STORED AS TEXTFILE " + "\n");
            sb.append("LOCATION \n");
            sb.append("'/EMS_Data/teamsun/temp/" + tableName.toUpperCase() + "'; \n");*/
            
			            System.out.println(sb.toString());
//			            System.out.println("insert into table ems_pmart." + tableName.toUpperCase() + " select * from ems_pmart.txt_" + tableName.toUpperCase() + ";");
//			            System.out.println(" drop table if exists ems_pmart.TXT_" + tableName.toUpperCase() + "; ");
//            System.out.println("select count(*) from ems_pdata_range." + tableName.toUpperCase() + ";");
//            System.out.println("select * from ems_pmart." + tableName.toUpperCase() + " limit 5;");
            
//            System.out.println(" select * from ems_pmart." + tableName.toUpperCase() + " limit 5;");
//			            System.out.println("hdfs dfs -rmr '/EMS_Data/teamsun/" + tableName.toUpperCase() + "'; \n");
            
//            System.out.println("insert into table ems_pmart." + tableName.toUpperCase() + " select " + queryCols.toString() + " from ems_pdata." + tableName.toUpperCase() + ";");
//            System.out.println("truncate table ems_pmart." + tableName.toUpperCase());
//            System.out.println("alter '" + tableName.toUpperCase() + "', {NAME => 'f', DATA_BLOCK_ENCODING => 'PREFIX', COMPRESSION => 'SNAPPY'}");
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
