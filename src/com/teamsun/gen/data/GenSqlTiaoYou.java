package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 生成调优后的Sql
 * 
 * @author Administrator
 *
 */
public class GenSqlTiaoYou 
{
	public static void main(String[] args) throws Exception
	{
		File dir = new File("d://bigData//SqlTiaoYou//");
		File[] fs = dir.listFiles();
		
        for (File f : fs)
        {
        	if (f.isDirectory())
        	{
        		String tableName = f.getName();
        		
        		File[] files = f.listFiles();
        		List<String> cols = new ArrayList<String>();
    			Map<String, String> keys = new LinkedHashMap<String, String>();
    			String sql = "";
        		
        		for (File file : files)
        		{
        			if (file.getName().equals("cols.txt"))
        			{
        				cols = readCols(file);
//        				System.out.println(cols);
        			}

        			if (file.getName().equals("keys.txt"))
        			{
        				keys = readKeys(file);
//        				System.out.println(keys);
        			}
        			
        			if (file.getName().equals("querySql.txt"))
        			{
        				sql = readQuerySql(file);
//        				System.out.println(sql);
        			}
        		}
        		
        		System.out.println("----------------" + tableName + " 开始优化---------------------");
        		
        		System.out.println("drop table ems_pmart." + tableName + ";");
        		System.out.println("drop table ems_pmart." + tableName.replace("view", "temp") + ";");
        		System.out.println("delete_index \'" + tableName + "\', \'IDX_1\'");
        		System.out.println("create \'" + tableName + "\','f'");
        		
        		genCreateHbaseHiveTable(cols, keys, tableName);
        		
        		genCreateResultHiveTable(tableName, sql);
        		
        		genCreateHbaseIndex(tableName);
        		
        		genImportData(tableName, cols, keys);
        		
        		genQuerySql(tableName, cols);
        	}
        }
	}

	private static void genQuerySql(String tableName, List<String> cols) 
	{
		StringBuffer sql = new StringBuffer();
		
		System.out.println("-----------------查询-----------------");
		System.out.println("set ngmr.exec.mode=local;");
		System.out.println("set ngmr.exec.mode=all;");
		
		sql.append("select " + "\n");
		
		String colStr = "";
		for (String col : cols)
		{
			colStr += col + "," + "\n";
		}
		
		sql.append(colStr.substring(0, colStr.length() - 2) + "\n");
		
		sql.append(" from ems_pmart." + tableName + " limit 5");
		
		System.out.println(sql.toString());
	}

	private static void genImportData(String tableName, List<String> cols,	Map<String, String> keys) 
	{
		StringBuffer sql = new StringBuffer();
		
		System.out.println("---------导数据----------------");
		
		sql.append("UPDATE ems_pmart." + tableName + " SET (" + "\n");
		sql.append("key," + "\n");
		
		String colStr = "";
		for (String col : cols)
		{
			colStr += col + "," + "\n";
		}
		
		sql.append(colStr.substring(0, colStr.length() - 2) + "\n");
		
		sql.append(")" + "\n");
		sql.append("=" + "\n");
		sql.append("(SELECT" + "\n");
		
		String key = "";
		for (Entry<String, String> entry : keys.entrySet()) 
		{
			key += "'" + entry.getKey() + "'" + "," + entry.getKey() + ",";
		}
		
		sql.append("named_struct(" + key.substring(0, key.length() - 1) + ") as key," + "\n");
		sql.append(colStr.substring(0, colStr.length() - 2) + "\n");
		sql.append("from ems_pmart." + tableName.replace("view", "temp") + " a " + "\n");
		sql.append(");");
		
		
		System.out.println(sql.toString());
	}

	private static void genCreateHbaseIndex(String tableName) 
	{
		System.out.println("---------建索引----------------");
		
		System.out.println("add_index '" + tableName  + "','IDX_1','COMBINE_INDEX|INDEXED=f:c1:8|f:c8:8|f:c9:8|rowKey:rowKey:50,UPDATE=true'");
	}

	private static void genCreateResultHiveTable(String tableName, String querySql) 
	{
		StringBuffer sql = new StringBuffer();
		
		System.out.println("--------------创建结果表-------------------");
		
		sql.append("create table ems_pmart." + tableName.replace("view", "temp") + "\n");
		sql.append("as"  + "\n");
		sql.append(querySql + " \n");
		
		
		System.out.println(sql.toString());
	}

	private static void genCreateHbaseHiveTable(List<String> cols,	Map<String, String> keys, String tableName) 
	{
		StringBuffer sql = new StringBuffer();
		
		System.out.println("-------------------创建Hbase外表-----------------");
		
		sql.append("create EXTERNAL table ems_pmart." +  tableName + " (" + "\n");
		
		String key = "";
		for (Entry<String, String> entry : keys.entrySet()) 
		{
			key += entry.getKey() + ":" + entry.getValue() + ",";
		}
		
		sql.append("key struct<" + key.substring(0, key.length() - 1) + ">," + "\n");
		
		String colStr = "";
		String hbaseCol = "";
		int i = 1;
		for (String col : cols)
		{
			colStr += col + " " + getType(col) + "," + "\n";
			hbaseCol += "f:c" + (i++) + ","; 
		}
		
		sql.append(colStr.substring(0, colStr.length() - 2) + "\n");
		sql.append(")" + "\n");
		sql.append("row format delimited collection items" + "\n");
		sql.append("terminated by '|'" + "\n");
		sql.append("STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'" + "\n");
		sql.append("with SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key," + "\n");
		sql.append(hbaseCol.substring(0, hbaseCol.length() -1) + "\") \n");
		sql.append("TBLPROPERTIES (\"hbase.table.name\" = \"" + tableName + "\");" + "\n");
		
		
		System.out.println(sql.toString());
	}

	private static String getType(String col) 
	{
		if (col.toUpperCase().contains("CNT"))
			return "int";
		else if (col.toUpperCase().contains("FEE"))
			return "double";
		else
			return "string";
	}

	private static String readQuerySql(File file)  throws Exception
	{
		InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "GBK");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		String querySql = "";
		
		while ((line = br.readLine()) != null) 
        {
			querySql += line + "\n";
        } 
		
		return querySql;
	}

	private static Map<String, String> readKeys(File file)  throws Exception
	{
		InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "GBK");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		Map<String, String> keys = new LinkedHashMap<String, String>();
		
		while ((line = br.readLine()) != null) 
        {
			String[] key = line.split(":");
			keys.put(key[0], key[1]);
        } 
		
		return keys;
	}

	private static List<String> readCols(File file) throws Exception
	{
		InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "GBK");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		List<String> cols = new ArrayList<String>();
		
		while ((line = br.readLine()) != null) 
        {
			cols.add(line);
        } 
		
		return cols;
	}
}
