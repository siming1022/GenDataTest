package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class ReadFileMain {

	public static void main(String[] args) throws Exception
	{
		File f = new File("D:\\A.txt");
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		while ((line = br.readLine()) != null) 
        {
			System.out.println(line);
        }
	}

}
