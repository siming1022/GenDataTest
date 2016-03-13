package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class GenCreateHiveTable {

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
        		 
        	//生成hive 外表建表语句
            String tableName = f.getName().replaceAll(".txt", "").replaceAll(".TXT", "");
            
            
//			            StringBuffer sb = new StringBuffer("");
            
//			            StringBuffer sb = new StringBuffer(" drop table if exists ems_pmart.TMP_" + tableName + "; \n");
//			            sb.append("create EXTERNAL table ems_pmart.TMP_" + tableName + "(");

            StringBuffer sb = new StringBuffer(" drop table if exists test." + tableName + "; \n");
            sb.append("create EXTERNAL table test." + tableName + "(");
            StringBuffer queryCols = new StringBuffer();
            
            while ((line = br.readLine()) != null) 
            {
        		String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
        		
        		sb.append(cols[0] + " " + changeType(cols[1]) + "," + " \n");
        		
        		queryCols.append(cols[0] + " AS " + cols[0] + ",");
        		
            }
            
            queryCols = new StringBuffer(queryCols.toString().substring(0, queryCols.length() - 1));
            sb = new StringBuffer(sb.substring(0, sb.length() - 3));
            sb.append(") " + "\n");
            
            /**
             * 建Orc外表
             *
             **/
            
//            sb.append("PARTITIONED BY (yearmonth string) \n" );
//            sb.append("CLUSTERED BY (" + "" + ") INTO " + 2 + " BUCKETS \n" );
//            sb.append("STORED AS ORC \n" );
//            sb.append("TBLPROPERTIES (\"transactional\"=\"true\"); \n\n" );
            
            
            
            
            
            /*sb.append("ROW FORMAT SERDE \n");
            sb.append(" 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'  \n");
            sb.append("STORED AS INPUTFORMAT \n");
            sb.append("'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' \n");
            sb.append("OUTPUTFORMAT \n");
            sb.append("'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'; \n");*/
            
            /**
             * 建文件文件外表
             *
             **/
            sb.append("ROW FORMAT DELIMITED  " + "\n");
            sb.append("FIELDS TERMINATED BY \'\\,\'" + "\n");
            sb.append("STORED AS INPUTFORMAT " + "\n");
            sb.append("'org.apache.hadoop.mapred.TextInputFormat'" + "\n");
            sb.append("OUTPUTFORMAT" + "\n");
            sb.append("'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \n");
            sb.append("LOCATION \n");
            sb.append("'/EMS_Data/teamsun/" + tableName.toUpperCase() + "'; \n");
            
            //insert into table ems_pmart.TB_CDE_CORE_SERVICES_73 select * from ems_pmart.TMP_TB_CDE_CORE_SERVICES_73;
            
//			            ROW FORMAT SERDE 
//			            'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
//			          STORED AS INPUTFORMAT 
//			            'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
//			          OUTPUTFORMAT 
//			            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
            
			            System.out.println(sb.toString());
//			            System.out.println("insert into table ems_pmart." + tableName.toUpperCase() + " select * from ems_pmart.txt_" + tableName.toUpperCase() + ";");
//			            System.out.println(" drop table if exists ems_pmart.TXT_" + tableName.toUpperCase() + "; ");
//            System.out.println("select count(*) from ems_pmart." + tableName.toUpperCase() + ";");
//            System.out.println("select * from ems_pmart." + tableName.toUpperCase() + " limit 5;");
            
//            System.out.println(" select * from ems_pmart." + tableName.toUpperCase() + " limit 5;");
//			            System.out.println("hdfs dfs -rmr '/EMS_Data/teamsun/" + tableName.toUpperCase() + "'; \n");
            
//            System.out.println("insert into table ems_pmart." + tableName.toUpperCase() + " select " + queryCols.toString() + " from ems_pdata." + tableName.toUpperCase() + ";");
//            System.out.println("truncate table ems_pmart." + tableName.toUpperCase());
            
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
