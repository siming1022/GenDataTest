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
public class GenJavaStatic
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
    		String fileName = transFileName(f.getName().replace(".def", ""));
    		boolean isCol = false;
    		
            String tableName = "\n\n\tpublic static class " + fileName + " {\n";
            System.out.println(tableName);
            System.out.println("\t\tpublic static final String CZFS = \"CZFS\"; // 操作方式");
            
            while ((line = br.readLine()) != null) 
            {
              	 String colName = line.substring(line.lastIndexOf("/", line.lastIndexOf("/") - 1) + 1, line.lastIndexOf("/"));
              	 
              	 if (!"1".equals(colName))
              	 {
              		 //生成hbase entity
              		 System.out.println("\t\tpublic static final byte[] " + colName + " = \"" + colName.toUpperCase() +"\".getBytes();");
              	 }
              	 
            }
            
            List<String> diffCols = Compare.compare(f.getName().replace(".def", ""));
            
            for (String diffCol : diffCols)
            {
            	System.out.println("\t\tpublic static final byte[] " + diffCol + " = \"" + diffCol.toUpperCase() + "\".getBytes();"); 
            }
            
//            System.out.println("\t\tpublic static final byte[] REC_AVAIL_Flag = \"REC_AVAIL_Flag\".getBytes();"); 
//            System.out.println("\t\tpublic static final byte[] Load_Date = \"Load_Date\".getBytes();");
//            System.out.println("\t\tpublic static final byte[] Load_Time = \"Load_Time\".getBytes();");
//            System.out.println("\t\tpublic static final byte[] Load_TimeStamp = \"Load_TimeStamp\".getBytes();"); 
               
             System.out.println("\t}");
             
             System.out.println("\tpublic static class " + fileName + "_List {}");
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
