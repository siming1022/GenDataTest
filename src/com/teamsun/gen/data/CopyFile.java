package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class CopyFile 
{
	public static void main(String[] args) throws Exception
	{
		File copyList = new File("D:\\bigData\\name.txt");
		InputStreamReader isr = new InputStreamReader(new FileInputStream(copyList), "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		while ((line = br.readLine()) != null) 
        {
			//line.trim().toUpperCase()
			String name = line.trim().toUpperCase() + ".TXT";
//			File sourceFile = new File("C:\\Users\\Administrator\\Desktop\\上海\\ddl\\" + name);
			File sourceFile = new File("D:\\bigData\\TD数据备份\\" + name);
			if (sourceFile.exists())
			{
				File destFile = new File("D:\\bigData\\导数据\\" + name);
				nioTransferCopy(sourceFile, destFile);
			}
			else
			{
				System.out.println(name + " is not exists");
			}
        }
	}
	private static void nioTransferCopy(File source, File target) {  
	    FileChannel in = null;  
	    FileChannel out = null;  
	    FileInputStream inStream = null;  
	    FileOutputStream outStream = null;  
	    try {  
	        inStream = new FileInputStream(source);  
	        outStream = new FileOutputStream(target);  
	        in = inStream.getChannel();  
	        out = outStream.getChannel();  
	        in.transferTo(0, in.size(), out);  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    } finally { 
	    	try {
				if (inStream != null)
					inStream.close();
				
				if (in != null)
					in.close();
				
				if (outStream != null)
					outStream.close();
				
				if (out != null)
					out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }  
	}  
	
	
}
