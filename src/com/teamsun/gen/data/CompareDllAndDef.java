package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CompareDllAndDef 
{
	public static void main(String[] args) throws Exception
	{
		File f = new File("d://bigData//name.txt");
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
    		 
        while ((line = br.readLine()) != null) 
        {
        	List<String> dllColList = new ArrayList<String>();
    		List<String> defColList = new ArrayList<String>();
        	File dllFile = new File("C:/Users/Administrator/Desktop/上海/ddl/" + line + ".txt");
        	File defFile = new File("C:/Users/Administrator/Desktop/上海/def/" + line + ".def");
//        	System.out.println(line);
        	InputStreamReader isrDllFile = new InputStreamReader(new FileInputStream(dllFile), "GBK");
    		BufferedReader dllBr = new BufferedReader(isrDllFile);
    		String dllLine = "";
    		boolean isCol = false;
    		
//    		System.out.println("------------文件：" + line + "------------------");
    		
    		while ((dllLine = dllBr.readLine()) != null) 
            {
//    			dllLine = dllLine.replace(" ", "");
    			if (!isCol && dllLine.contains("(")) 
    			{
    				isCol = true;
    				continue;
    			}
    			
    			if (isCol && dllLine.contains("INDEX"))
    			{
    				break;
    			}
    			
    			if (isCol)
    			{
    				dllLine = dllLine.substring(dllLine.indexOf(" ") + 6, dllLine.indexOf(" ",   dllLine.indexOf(" ") + 6));
    				
//    				if (dllLine.toUpperCase().equals("REC_AVAIL_FLAG") 
//    						|| dllLine.toUpperCase().equals("LOAD_DATE")
//    						|| dllLine.toUpperCase().equals("LOAD_TIME")
//    						|| dllLine.toUpperCase().equals("LOAD_TIMESTAMP")
//    						|| dllLine.toUpperCase().equals("DATA_FIRST_UPLOAD_TIMESTAMP"))
//    				{
//    					continue;
//    				}
    				dllColList.add(dllLine.toUpperCase());
//    				System.out.println(dllLine);
    			}
            }
    		
        	InputStreamReader isrDefFile = new InputStreamReader(new FileInputStream(defFile), "GBK");
    		BufferedReader defBr = new BufferedReader(isrDefFile);
    		String defLine = "";
    		
    		while ((defLine = defBr.readLine()) != null) 
            {
    			String def = defLine.substring(defLine.lastIndexOf("/", defLine.lastIndexOf("/") - 1) + 1, defLine.lastIndexOf("/"));
    			if (!"1".equals(def))
    				defColList.add(def.toUpperCase());
            }
    		
    		List<String> errDefList = new ArrayList<String>();
    		//比较在Def文件中，哪些字段在表结构中没有
            for (String def : defColList)
            {
            	if (!dllColList.contains(def))
            	{
            		errDefList.add(def);
            	}
            }
            
            if (errDefList.size() > 0)
            {
            	System.out.println("在Def文件：" + line + ".def中此字段："+ errDefList.toString() + " 在表结构" + line + ".txt文件中没有");
            }
            
            List<String> errDdlList = new ArrayList<String>();
            //比较在表结构文件中，哪些字段在中Def没有
            for (String dll : dllColList)
            {
            	if (!defColList.contains(dll))
            	{
            		errDdlList.add(dll);
            	}
            }
            
            if (errDdlList.size() > 0)
            {
            	System.out.println("在表结构：" + line + ".txt中此字段："+ errDdlList.toString() + " 在" + line + ".def文件中没有");
            }
            
            isrDllFile.close();
            dllBr.close();
            isrDefFile.close();
            defBr.close();
        }
        
//        System.out.println(defColList.toString());
//        System.out.println(dllColList.toString());
	}
}
