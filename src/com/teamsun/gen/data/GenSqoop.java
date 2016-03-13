package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

/**  
 * 读取文件生成Sqoop命令
 * 
 * @Title: Test11.java
 * @Description: TODO
 * @author: Administrator
 * @date: 2015年6月3日 下午6:01:53
 */
public class GenSqoop 
{

	public static void main(String[] args)
	{
		try 
		{
			File f = new File("D://bigData//sqoop.txt");
			InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
			BufferedReader br = new BufferedReader(isr);
			String line = "";
			
			while ((line = br.readLine()) != null) 
			{
				String[] s = line.split("\t");
				String tableName = s[0];
				String ip = s[1];
				String orcl = s[2]; 
				String userName = s[3]; 
				String pwd = s[4];
				//如果Rowkey由多个字段组成，在查询语句中将这些字段拼起来，作为一个单独的字段
				String myRowKey = s[5];
				String querySql = s[6];
				
				System.out.println(tableName);
//				System.out.println("echo 'begin run " + tableName + " at '`date -d\"0 day ago\" +\"%F %H:%M:%S\"`");
//				System.out.println(getToHbase(tableName, ip, "1521", orcl, userName, pwd, myRowKey, querySql));
//				System.out.println("echo 'end run TB_OFR_CUST_REP_SECT at '`date -d\"0 day ago\" +\"%F %H:%M:%S\"` \n\n");
				writeToFile(tableName.trim(), getToHbase(tableName, ip, "1521", orcl, userName, pwd, myRowKey, querySql));
//				System.out.println(getToHFDS(tableName, ip, "1521", orcl, userName, pwd, "/data/fdr", querySql, myRowKey));
			}
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void writeToFile(String tableName, String string) 
	{
		try 
		{
			File f = new File("d://bigData//sqoop//run_" + tableName + ".sh");
			PrintWriter wr = new PrintWriter(f);
			wr.print("echo 'begin run " + tableName + " at '`date -d\"0 day ago\" +\"%F %H:%M:%S\"` \n");
			wr.print(new String(string.getBytes("UTF-8")) + "\n");
			wr.print("echo 'end run " + tableName + " at '`date -d\"0 day ago\" +\"%F %H:%M:%S\"` \n\n");
			wr.close();
		} 
		catch (Exception e) 
		{
		}
	}
	
	static String getToHbase(String tableName, String ip, String port, String orcl, String userName, String pwd, String rowKey, String querySql)
	{
//		jdbc:oracle:thin:@10.3.18.10:1521:rdb
		return "sqoop import --connect jdbc:oracle:thin:@" + ip + ":" + port + ":" + orcl 
				+ " --username " + userName + "   --password " + pwd //+ "  --table  " + tableName
				+ " --query \"" + querySql + "  WHERE \\$CONDITIONS \""
				+ "  --hbase-create-table "
				+ "  --hbase-table " + tableName
				+ "  --column-family f "
				+ "  --hive-drop-import-delims "
				+ " --null-string '\\\\N' "
				+ " --null-non-string '\\\\N' "
				+ "  --hbase-row-key ROWKEY"
				+ "  --m 1";
	}

	static String getToHFDS(String tableName, String ip, String port, String orcl, String userName, String pwd, String dir, String querySql, String splitBy)
	{
//		/'SELECT a.*, b.* FROM a JOIN b on (a.id == b.id) WHERE $CONDITIONS' 
		return "sqoop import --connect jdbc:oracle:thin:@//" + ip + ":" + port + "/" + orcl 
				+ " --username " + userName + "   --password " + pwd //+ "  --table  " + tableName 
				+ " --query \"" + querySql + "  WHERE rownum < 5 and \\$CONDITIONS \""
				+ " --target-dir " + dir
//				+ " --fields-terminated-by ',' "
				+ " --split-by " + splitBy
				+ " --hive-drop-import-delims "
				+ " --null-string '\\\\N' "
				+ " --null-non-string '\\\\N' "
				+ " --m 1 ";
	}

}
