package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;



/**  
 * 读取单个Def文件生成Field和hbase代码
 * 
 * @Title: Test13.java
 * @Description: TODO
 * @author: Administrator
 * @date: 2015年6月3日 下午6:03:11
 */
public class GenJavaHBase
{
	
	public static void main(String[] args) throws Exception    
    {
		File dir = new File("D://bigData//defs/");
		
		File[] fs = dir.listFiles();
        
        for (File f : fs)
        {
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
    		int i = 0;
        		 
        	//生成hbase put 代码
            String tableName = f.getName().replace(".def", "");
            String family = "f";
            String entityName = "EMS_Field_List." + transFileName(tableName);
            String v = "msg.get" + transFileName(tableName) + "().get";
               
            StringBuffer sb = new StringBuffer(tableName + ".put(new Put(rowkey.getBytes())");
            StringBuffer rowkey = new StringBuffer();
            while ((line = br.readLine()) != null) 
            {
              	 String s = line.substring(0, 1);
              	 String colName = line.substring(line.lastIndexOf("/", line.lastIndexOf("/") - 1) + 1, line.lastIndexOf("/"));
              	 
              	 rowkey.append(v + trans(colName) + "()) + \"~\" + ");
              	 
              	//生成hbase put 代码
             	 sb.append(".add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(" + v + trans(colName) + "()))" + "\n");
            }
            
//            System.out.println(rowkey.toString());
               
            List<String> diffCols = Compare.compare(f.getName().replace(".def", ""));
            
            for (String diffCol : diffCols)
            {
            	sb.append(".add(" + family + ", " + entityName + "." + diffCol + ", Bytes.toBytes(" + v + trans(diffCol) + "()))" + "\n"); 
            }
            
            System.out.println(sb.toString() + ");");
            
            
            System.out.println("\n\n\n\n\n");
        }
	}

	/**
	 * 将def文件名转换成JAVA的驼峰命名
	 * 
	 * @Title: Test13
	 * @Description: TODO
	 * @author: Administrator
	 * @param fileName
	 * @return
	 * @throws: 
	 */
	private static String transFileName(String fileName) 
	{
		String res = "";
		if (fileName.indexOf("_") != -1)
		{
			String[] tempStrs = fileName.split("_");

			if (tempStrs.length == 3)
			{
				String[] dest = new String[2];
				System.arraycopy(tempStrs, 1, dest, 0, 2);
				res = transFileName(dest);
			}
			else
			{
				String[] dest = new String[tempStrs.length - 2];
				System.arraycopy(tempStrs, 2, dest, 0, tempStrs.length - 2);
				res = transFileName(dest);
			}
		}
		
		return res;
	}

	private static String transFileName(String[] tempStrs) 
	{
		String res = "";
		
		for (int i = 0; i < tempStrs.length; i++)
		{
			res += (tempStrs[i].substring(0, 1).toUpperCase() + tempStrs[i].substring(1, tempStrs[i].length()).toLowerCase());
		}
		
		return res;
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
