package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Compare {

	public static void main(String[] args) 
	{
	}
	
	public static List<String> compare(String fileName) throws Exception
	{
		File defFile = new File("d://bigData//defs//" + fileName + ".def");
		File dllFile = new File("d://bigData//ddls//" + fileName + ".TXT");
		List<String> dllColList = new ArrayList<String>();
		List<String> defColList = new ArrayList<String>();
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(defFile), "GBK");
		BufferedReader br = new BufferedReader(isr);
		String defLine = "";
    		 
        while ((defLine = br.readLine()) != null) 
        {
        	String def = defLine.substring(defLine.lastIndexOf("/", defLine.lastIndexOf("/") - 1) + 1, defLine.lastIndexOf("/"));
			
        	if (!"1".equals(def))
				defColList.add(def.toUpperCase());
			
    		defColList.add(def.toUpperCase());
//    		System.out.println(def);
        }
        
        InputStreamReader isrDllFile = new InputStreamReader(new FileInputStream(dllFile), "GBK");
		BufferedReader dllBr = new BufferedReader(isrDllFile);
		String dllLine = "";
		while ((dllLine = dllBr.readLine()) != null) 
		{
			String tempDllStr = dllLine.substring(6, dllLine.length());
			tempDllStr = tempDllStr.substring(0, tempDllStr.indexOf(" "));
//			System.out.println(tempDllStr);
			
			dllColList.add(tempDllStr);
		}
		
		List<String> diffCols = new ArrayList<String>();
		for (String dllStr : dllColList)
		{
			if (!defColList.contains(dllStr.toUpperCase()))
			{
				diffCols.add(dllStr);
//				System.out.println("\t\tpublic static final byte[] " + dllStr + " = \"" + dllStr + "\".getBytes();"); 
			}
		}
		
		return diffCols;
	}

}
