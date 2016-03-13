package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;


/**  
 * 读取数据库表结构文件生成protobuf数据
 * 
 * @Title: Test12.java
 * @Description: TODO
 * @author: Administrator
 * @date: 2015年6月3日 下午6:02:44
 */
public class GenProtobufByDataBase 
{

	public static void main(String args[]) {
		try 
		{
			File f = new File("D://Noname1.txt");
			InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
			BufferedReader br = new BufferedReader(isr);
			String line = "";
			
			String tableName = "TB_FACT_PDA_MESSAGE_R_MID";
	         String family = "f";
	         String entityName = "EMS_Field_List.FeedbackMid";
	         String v = "msg.getFeedbackMid().get";
	         
	         int i = 0;
	         
	         StringBuffer sb = new StringBuffer(tableName + ".put(new Put(rowkey.getBytes())");
	         
			while ((line = br.readLine()) != null) 
			{
				String colName = line.split(" ")[0];
				
//				System.out.println("optional string " + colName + " = " + (++i) + ";");
				
//				System.out.println("public static final byte[] " + colName + " = \"" + colName +"\".getBytes();");
				
				sb.append(".add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(" + v + trans(colName) + "()))" + "\n");
			}
			
			
			System.out.println(sb.toString() + ");");
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 将def文件中的字段名转换成JAVA的驼峰命名
	 * 
	 * @Title: Test13
	 * @Description: TODO
	 * @author: Administrator
	 * @param colName
	 * @return
	 * @throws: 
	 */
	private static String trans(String colName) 
	{
		String res = "";
		if (colName.indexOf("_") != -1)
		{
			String[] tempStrs = colName.split("_");
			for (int i = 0; i < tempStrs.length; i++)
			{
				if (i == 0)
				{
					res += tempStrs[i];
				}
				else
				{
					String tempStr = tempStrs[i];
					
					boolean isAllUpper = true;
					for (int s = 0; s < tempStr.length(); s++)
					{
						if (!Character.isUpperCase(tempStr.charAt(s)))
						{
							isAllUpper = false;
						}
					}
					
					if (isAllUpper)
					{
						res += tempStrs[i];
					}
					else
					{
						res += tempStr.substring(0, 1).toUpperCase() + tempStr.substring(1, tempStr.length()).toLowerCase();
					}

				}
			}
		}
		
		return res;
	}
}
