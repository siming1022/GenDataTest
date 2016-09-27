package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataRepMain
{
	public static void main(String[] args) throws Exception
	{
		File f = new File("C:\\Users\\Administrator\\Desktop\\dataRep\\datarep.txt");
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		Map<String, String> tbs = getTb();
		Map<String , List<String>> queueReps = new HashMap<String, List<String>>();
		Set<String> set = new HashSet<String>();
		while ((line = br.readLine()) != null)
		{
			String[] queue = line.split("\t");
			set.add(queue[0]);
			if (queueReps.containsKey(queue[0]))
			{
				queueReps.get(queue[0]).add(queue[1].trim());
			}
			else
			{
				List<String> ls = new ArrayList<String>();
				ls.add(queue[1]);
				
				queueReps.put(queue[0], ls);
			}
		}
		
		System.out.println("rep queue size is " + set.size());
		
		for (String queue : set)
		{
			if (!tbs.containsKey(queue))
			{
				continue;
			}
			
			System.out.println("nohup beeline -u 'jdbc:hive2://10.1.200.24:10000/default' -f $DIR/" + tbs.get(queue).split("-")[0].toUpperCase() + ".sql &");
			/*File tableDef = new File("C:\\Users\\Administrator\\Desktop\\上海\\def\\" + tbs.get(queue).split("-")[0].toUpperCase() + ".def");
			
			if (tableDef.exists())
			{
				String sql = getSql(tableDef, queueReps, tbs.get(queue).toUpperCase(), queue);
				System.out.println(sql);
			}*/
		}
		//得到相关表信息
		/*for (String queue : set)
		{
//			System.out.println(queue);
			if (!tbs.containsKey(queue))
			{
				System.out.println("queue is not table ddl " + queue);
				continue;
			}
			File tableDdl = new File("C:\\Users\\Administrator\\Desktop\\上海\\def\\" + tbs.get(queue).toUpperCase() + ".def");
//			File tableDdl = new File("C:\\Users\\Administrator\\Desktop\\上海\\ddl\\" + tbs.get(queue).toUpperCase() + ".txt");
			
			if (!tableDdl.exists())
			{
//				File tableDdl2 = new File("C:\\Users\\Administrator\\Desktop\\上海\\def\\" + tbs.get(queue).toUpperCase() + ".DEF");
//				File tableDdl2 = new File("C:\\Users\\Administrator\\Desktop\\上海\\ddl\\" + tbs.get(queue).toUpperCase() + ".TXT");
				
//				if (!tableDdl2.exists())
					System.out.println("def is not exists " + tbs.get(queue).toUpperCase() + " " + queue);
//				else
//					getpartitionBy(tableDdl2, tbs.get(queue).toUpperCase(), queue);
			}
			else
			{
//				getpartitionBy(tableDdl, tbs.get(queue).toUpperCase(), queue);
			}
		}*/
	}
	
	private static String getSql(File tableDef, Map<String, List<String>> queueReps, String tableInfo, String queueNum) 
	{
		StringBuffer sql = new StringBuffer();
		
		InputStreamReader isr = null;
		BufferedReader br = null;
		try 
		{
			isr = new InputStreamReader(new FileInputStream(tableDef), "UTF-8");
			br = new BufferedReader(isr);
			String line = "";
			String queryCols = "";
			while ((line = br.readLine()) != null)
			{
				if (line.length() > 12)
				{
					String col = line.substring(12, line.indexOf("/", 12));
	        		
					queryCols += "NVL(" + col + ", '') AS " + col + ","; 
				}
			}
			queryCols = queryCols.substring(0, queryCols.length() - 1);
			
			String partitionCol = tableInfo.split("-")[1];
			String tbName = tableInfo.split("-")[0];
			
			for (String date : queueReps.get(queueNum))
			{
				String hdfsLoc = "/EMS_Data/DATA_BAK/" + queueNum + "/" + date;
				String command = "INSERT OVERWRITE DIRECTORY '" + hdfsLoc + "' row format delimited fields terminated by '\\t' SELECT " + queryCols + " FROM EMS_PDATA_RANGE." + tbName + " WHERE " + partitionCol + " = '" + date + "';";
//				System.out.println(command);
//				writeToFile(tbName, command);
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println(queueNum);
		}
		finally
		{
			try {
				if (isr != null)
					isr.close();
				if (br != null)
					br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return sql.toString();
	}

	private static void getpartitionBy(File tableDdl, String tbName, String queue) throws Exception
	{
		InputStreamReader isr = new InputStreamReader(new FileInputStream(tableDdl), "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		String col = "";
		boolean hasLoadDate = false;
		while ((line = br.readLine()) != null)
		{
			if (line.contains("PARTITION BY RANGE_N("))
			{
//				System.out.println(line);
				int beginIndex = line.indexOf("PARTITION BY RANGE_N(")+21;
				col = line.substring(beginIndex, line.indexOf(" ", beginIndex));
				break;
			}
			
			if (line.toUpperCase().contains("LOAD_DATE"))
				hasLoadDate = true;
		}
		
		if (!"".equals(col))
		{
			System.out.println(queue + "\t" + tbName.toUpperCase() + "\t" + col);
		}
		else
		{
			if (hasLoadDate)
			{
				System.out.println(queue + "\t" + tbName.toUpperCase() + "\t" + "LOAD_DATE");
			}
			else
			{
				System.out.println("not partition " + tbName + " " + queue);
			}
		}
	}

	//PARTITION BY

	private static Map<String, String> getTb() throws Exception
	{
		Map<String, String> tbs = new HashMap<String, String>();
		File f = new File("C:\\Users\\Administrator\\Desktop\\dataRep\\tableinfo.txt");
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		while ((line = br.readLine()) != null)
		{
			String[] tableArr = line.split("\t");
			tbs.put(tableArr[0].trim(), tableArr[1].trim() + "-" + tableArr[2].trim());
		}
		
		return tbs;
	}
	
	private static void writeToFile(String tableName, String string) 
	{
		try 
		{
			File f = new File("C:\\Users\\Administrator\\Desktop\\dataRep\\sql\\" + tableName + ".sql");
			FileWriter wr = new FileWriter(f, true);
			wr.write(new String(string.getBytes("UTF-8")) + "\r\n");
			wr.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
}
