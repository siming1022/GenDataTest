package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;



/**  
 * 批量读取Def文件后生成protobuf文件
 * 
 * @Title: GenProtobuf.java
 * @Description: TODO
 * @author: Administrator
 * @date: 2015年6月3日 下午6:03:11
 */
public class GenProtobuf 
{
	
	public static void main(String[] args) throws Exception    
    {
		File f = new File("D://bigData//defs");
		
		String entityNames = "message EMS_HTTP_MSG {\n";
		String packName = "package io.transwarp.ems.tracker.entity;\n\n" +
						  "option java_package = \"io.transwarp.ems.tracker.entity\";\n" +
						  "option java_outer_classname = \"EMS_ESB_MSG_Protos\";\n\n\n";
		
		int entityIndex = 1;
		String annotationName = "";
         
//		System.out.println(packName);
		
         File[] fs = f.listFiles();
         
         for (File file : fs)
         {
        	 if (file.isDirectory())
        	 {
        		 continue;
        	 }
        	 annotationName = "//" + file.getName().replace(".def", "");
        	 String protName =  trans(file.getName().replace(".def", ""));
    		 InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "GBK");
    		 BufferedReader br = new BufferedReader(isr);
    		 String line = "";
    		 int i = 1;
    		 
    		 StringBuffer protSb = new StringBuffer(annotationName + "\n" + "message " + protName + " {\n");
    		 protSb.append("\toptional string CZFS = 1; // 操作方式\n");
           
    		 while ((line = br.readLine()) != null) {
    			 String s = line.substring(0, 1);
//    			 System.out.println(line);
    			 String colName = line.substring(line.lastIndexOf("/", line.lastIndexOf("/") - 1) + 1, line.lastIndexOf("/"));
          	 
    			 if (!"1".equals(colName))
    			 {
    				 //生成protobuf
    				 protSb.append("\t" + ("P".equals(s)?"required":"optional") + " string " + colName + " = " + (++i) + ";\n");
    			 }
    		 }
           
//    		 System.out.println("\n\n");
    		 protSb.append("}\t\n\n");
           
    		 protSb.append("message " + protName + "_List {\n")
           		.append("\trepeated " + protName + " " + (protName.substring(0, 1).toLowerCase() + protName.substring(1)) + "List = 1; \n")
	           .append("}");
    		 
    		 entityNames += "\toptional " + protName + " " + (protName.substring(0, 1).toLowerCase() + protName.substring(1)) + " = " + (entityIndex++) + ";\n";
    		 System.out.println(protSb.toString());
         }
         
         entityNames += "\trequired string queueName = 99; // 数据来源队列名 \n}"; 
    		
         System.out.println(entityNames);
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
	private static String trans(String fileName) 
	{
		String res = "";
		if (fileName.indexOf("_") != -1)
		{
			String[] tempStrs = fileName.split("_");

			if (tempStrs.length == 3)
			{
				String[] dest = new String[2];
				System.arraycopy(tempStrs, 1, dest, 0, 2);
				res = trans(dest);
			}
			else
			{
				String[] dest = new String[tempStrs.length - 2];
				System.arraycopy(tempStrs, 2, dest, 0, tempStrs.length - 2);
				res = trans(dest);
			}
		}
		
		return res;
	}

	private static String trans(String[] tempStrs) 
	{
		String res = "";
		
		for (int i = 0; i < tempStrs.length; i++)
		{
			res += (tempStrs[i].substring(0, 1).toUpperCase() + tempStrs[i].substring(1, tempStrs[i].length()).toLowerCase());
		}
		
		return res;
	}

}
