package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RenameFile {

	public static void main(String[] args) throws Exception
	{
		File dir = new File("D://bigData//改表名/");
		
		File[] fs = dir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) 
			{
				if (name.toUpperCase().endsWith(".SQL"))
					return true;
				
				return false;
			}
		});
		
		for (File f : fs)
        {
        	InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
    		BufferedReader br = new BufferedReader(isr);
    		String line = "";
    		int i = 0;
    		StringBuffer sb = new StringBuffer();
            Map<String, String> m = new HashMap<String, String>();
            while ((line = br.readLine()) != null) 
            {
//            	System.out.println(f.getName());
            	
            	String oldName = "";
            	String newName = "";
            	try {
					String tbName = "";
					if (line.toUpperCase().contains("TEMPORARY"))
					{
//						System.out.println(line);
						int index = line.indexOf("STORED AS");
						String res = "";
						if (index == -1)
						{
							res = line +  "_" + f.getName().toUpperCase().replaceAll(".SQL", "") + " STORED AS ORC";
//							System.out.println(res);
						}
						else
						{
							String tmpBegin = line.substring(0, index - 1);
							String tmpEnd = line.substring(index, line.length());
							while (tmpBegin.lastIndexOf(" ") == tmpBegin.length() - 1)
							{
								tmpBegin= tmpBegin.substring(0, tmpBegin.length() - 1);
							}
							res = tmpBegin + "_" + f.getName().toUpperCase().replaceAll(".SQL", "") + " " + tmpEnd;
							int tmpIdx = res.indexOf("TABLE");
							
							newName = res.substring(tmpIdx + 6, res.indexOf("STORED")).replaceAll(" ", "").replaceAll("  ", ""); 
							
						}
						
						int tbIndex = line.indexOf("TMP_");
						
						if (tbIndex == -1)
						{
							tbIndex = line.indexOf("tmp_");
							
							if (tbIndex == -1)
								tbIndex = line.indexOf("VT_");
							
							if (tbIndex == -1)
								tbIndex = line.indexOf("DLV_");

							if (tbIndex == -1)
								tbIndex = line.indexOf("TEMP_");

							if (tbIndex == -1)
								tbIndex = line.indexOf("TB_");
						}
						
						oldName = line.substring(tbIndex, line.toUpperCase().indexOf("STORED AS ORC")).replaceAll(" ", "");
//						System.out.println(oldName);
//						System.out.println(newName);
						
						m.put(oldName, newName);
						System.out.println(res);
						sb.append(res + " \n"); 
//        			System.out.println(index);
//        			System.out.println(tmpBegin);
//        			System.out.println(tmpEnd);
//        			System.out.println(res);
					}
					else
					{
						sb.append(line + " \n");
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(0);
				}
              	 
            }
            
            String tt = sb.toString();
            StringBuffer sb2 = new StringBuffer();
            for (Entry<String, String> entry : m.entrySet()) 
    		{
    			String oldName = entry.getKey().toUpperCase();
    			String newName = entry.getValue().toUpperCase().replaceAll(" ", "");
    			tt = tt.replaceAll(" " + oldName + ";", " " + newName + ";");
    			tt = tt.replaceAll(" " + oldName + " ;", " " + newName + ";");
    			tt = tt.replaceAll(" " + oldName + "[)]", " " + newName + ")");
    			tt = tt.replaceAll(" " + oldName + " ", " " + newName + " ");
    			
    		}
            writeToFile(f.getName(), tt.toString());
        }
	}
	
	private static void writeToFile(String fileName, String string) 
	{
		try 
		{
			File f = new File("D://bigData//改表名//改后//" + fileName);
			PrintWriter wr = new PrintWriter(f);
			wr.println(new String(string.getBytes("UTF-8")));
			wr.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}
