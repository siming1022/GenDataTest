package com.teamsun.gen.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


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
			File dir = new File("D:\\bigData\\导数据\\");
			 File[] fs = dir.listFiles();
			 int i = 1;
	         for (File f : fs)
	         {
	        	 InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
	        	 BufferedReader br = new BufferedReader(isr);
	        	 
	        	 String oldTbName = f.getName().toLowerCase().replaceAll(".txt", "").toUpperCase();
	        	 String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
//	        	 genProtobuf(f);
//	        	 genJavaStatic(f);
	        	 genHbaseInput(f);
//	        	 genLSHbaseInput(f);
//	        	 genEnum(oldTbName, i);
//	        	 copyPropFile("D:\\data-workspace\\EMS_HTTP_GJ\\conf\\loader\\", "url_list.properties");
	        	 
//	        	 System.out.println("\tHTable " + oldTbName + ";");
//	        	 System.out.println(oldTbName + " = new HTable(this.conf, \"" + oldTbName + "\");");
//	        	 System.out.println(oldTbName + ".setAutoFlush(false, true);");
	        	 
	        	 
//	        	 System.out.println("optional " + tableName + " " + (tableName.substring(0, 1).toLowerCase() + tableName.substring(1, tableName.length())) + " = " + i + ";" );
	        	 
//	        	 System.out.print("\"" + tableName + "\",");
	        	 
	        	 /*System.out.println("else if(queueName.equalsIgnoreCase(\"" + oldTbName + "\"))");
	        	 System.out.println("{ ");
    			 System.out.println("\tEMSMain.hyApp.put" + oldTbName + "2HyperBase(msgArr, msgQueue);");
				 System.out.println("}");*/

	        	 /*System.out.println("else if(queueName.equalsIgnoreCase(EMSQueueEnum." + oldTbName.replaceAll("_", "").toUpperCase() + ".toString()))");
	        	 System.out.println("{ ");
    			 System.out.println("\tEMSMain.hyApp.put" + oldTbName + "2HyperBase(msgArr, msgQueue);");
				 System.out.println("}");*/
	        	 
	        	 /*System.out.println("else if(queueName.equalsIgnoreCase(\"" + oldTbName + "\"))");
	        	 System.out.println("{ ");
    			 System.out.println("\tbuilder = EMS_HTTP_MSG_Protos." + tableName + ".newBuilder();");
    			 System.out.println("\tdesc = EMS_HTTP_MSG_Protos." + tableName + ".Builder.getDescriptor();");
				 System.out.println("}");*/

	        	 /*System.out.println("else if(queueName.equalsIgnoreCase(EMSQueueEnum." + oldTbName.replaceAll("_", "").toUpperCase() + ".toString()))");
	        	 System.out.println("{ ");
    			 System.out.println("\tbuilder = EMS_HTTP_MSG_Protos." + tableName + ".newBuilder();");
    			 System.out.println("\tdesc = EMS_HTTP_MSG_Protos." + tableName + ".Builder.getDescriptor();");
				 System.out.println("}");*/
	        	 
				 /*System.out.println("else if(queueName.equalsIgnoreCase(\"" + oldTbName + "\"))");
				 System.out.println("{ ");
				 System.out.println("\tesb_builder.setQueueName(EMSQueueEnum." + oldTbName.replaceAll("_", "") + ".toString());");
				 System.out.println("\tesb_builder.set" + tableName + "(((EMS_HTTP_MSG_Protos." + tableName + ".Builder) builder).build());");
				 System.out.println("}");*/

				 /*System.out.println("else if(queueName.equalsIgnoreCase(EMSQueueEnum." + oldTbName.replaceAll("_", "").toUpperCase() + ".toString()))");
				 System.out.println("{ ");
				 System.out.println("\tesb_builder.setQueueName(EMSQueueEnum." + oldTbName.replaceAll("_", "") + ".toString());");
				 System.out.println("\tesb_builder.set" + tableName + "(((EMS_HTTP_MSG_Protos." + tableName + ".Builder) builder).build());");
				 System.out.println("}");*/
	        	 
//	        	 System.out.println("" + oldTbName + "=/EMS_Data/teamsun/emsFilePush/ls/" + oldTbName + "_.dat");
	        	 
	        	 
        		 i++;
	         }
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	

	private static void copyPropFile(String sourcePath, String fileName) throws Exception
	{
		Map<String, String[]> tbInfos = getTbInfos();
		File sourceFile = new File(sourcePath + File.separator + fileName);
		for (Entry<String, String[]> e : tbInfos.entrySet())
		{
			String queueNum = e.getValue()[0];
			File destFile = new File(sourcePath + File.separator + queueNum + "_" + fileName);
			if (!destFile.exists())
			{
				System.out.println("create file " + queueNum);
				Files.copy(sourceFile.toPath(), destFile.toPath());
			}
		}
	}




	private static void genEnum(String tbName, int i) throws Exception
	{
		Map<String, String[]> tbInfos = getTbInfos();
		String oldTbName = tbName.toUpperCase().replaceAll("_", "").toUpperCase();
		/*if (tbInfos.containsKey(tbName))
			System.out.println(oldTbName + "(\"" + oldTbName + "\", \"" + tbInfos.get(tbName)[2] + "\", " + "\"" + tbInfos.get(tbName)[0] + "\", " + (--i) + "),");
		else
			System.out.println(oldTbName + "(\"" + oldTbName + "\", \"未知\", " + "\"9999\", " + (--i) + "),");*/
		
		/*System.out.println("case " + (--i) + ":");
		System.out.println("\treturn " + tbName.toUpperCase().replaceAll("_", "") + ";");*/

		if (tbInfos.containsKey(tbName))
		{
			System.out.println("else if(queueNum.equals(\"" + tbInfos.get(tbName)[0] + "\"))");
			System.out.println("{");
			System.out.println("\treturn " + tbName.toUpperCase().replaceAll("_", "") + ";");
			System.out.println("}");
			
		}
		else
		{
			System.out.println("else if(queueNum.equals(\"999\"))");
			System.out.println("{");
			System.out.println("\treturn " + tbName.toUpperCase().replaceAll("_", "") + ";");
			System.out.println("}");
		}
//		else if(queueNum.equals("05001"))
//		  {
//		      return BAGMAILRELAREC;
//		  }
		i+=2;
	}



	private static Map<String, String[]> getTbInfos() throws Exception
	{
		Map<String, String[]> tbInfos = new HashMap<String, String[]>();
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream("d://bigData//tbInfo.txt"), "UTF-8");
	   	BufferedReader br = new BufferedReader(isr);
	   	String line = "";
	   	
	   	while ((line = br.readLine()) != null) 
	   	{
	   		String[] info = line.split("\t");
	   		tbInfos.put(info[1], info);
	   	}
	   	
	   	return tbInfos;
	}



	private static void genHbaseInput(File f) throws Exception
	{
		 InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
	   	 BufferedReader br = new BufferedReader(isr);
	   	 String line = "";
	   	 
	   	 String oldTableName = f.getName().toLowerCase().replaceAll(".txt", "").toUpperCase();
		 String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
	   	
		 String family = "f";
    	 String entityName = "EMS_Field_List." + tableName;
    	 String rowkeys = "";
//    	 String v = "msg.get" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1, tableName.length()).toLowerCase() + "().get";
    	 String v = "msg.get" + tableName + "().get";
    	 StringBuffer sb = new StringBuffer("\t\t\t" + oldTableName + ".put(new Put(rowkey.getBytes())\n");

    	 while ((line = br.readLine()) != null) 
    	 {
    		 String colName = line.substring(line.indexOf("      ")+6).split(" ")[0].toUpperCase();
    		 sb.append("\t\t\t\t.add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(" + v + tranCol(colName).toUpperCase() + "()))" + "\n");
    		 
    		 rowkeys += (v + tranCol(colName).toUpperCase() + "()") + " + \"~\" + \n \t\t\t\t";
    	 }
    	 
	   	 System.out.println("public void put" + oldTableName + "2HyperBase(List<EMS_HTTP_MSG_Protos.EMS_HTTP_MSG> msgArr, BlockingQueue<EMS_HTTP_MSG_Protos.EMS_HTTP_MSG> msgQueue)");
	   	 System.out.println("{");
	   	 System.out.println("\tfor (EMS_HTTP_MSG_Protos.EMS_HTTP_MSG msg : msgArr)");
	   	 System.out.println("\t{");
	   	 System.out.println("\t\ttry");
	   	 System.out.println("\t\t{");
	   	 System.out.println("\t\t\tString rowkey=new StringBuffer(" + rowkeys.substring(0, rowkeys.length() - 15) + ").toString();\n\n");
	   	 System.out.println("\t\t\tDate d = new Date();");
	   	 System.out.println("\t\t\tDateFormat df = new SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss\");");
	   	 System.out.println("\t\t\tString ds = df.format(d);");
	   	 
	   	 
	   	sb.append("\t\t\t\t.add(" + family + ", " + entityName + ".REC_AVAIL_FLAG, REC_AVAILF_FLAG_VALID)" + "\n");
	   	sb.append("\t\t\t\t.add(" + family + ", " + entityName + ".LOAD_DATE, Bytes.toBytes(ds.substring(0,4)+\"\"+ds.substring(5,7)+\"\"+ds.substring(8,10)))" + "\n");
	   	sb.append("\t\t\t\t.add(" + family + ", " + entityName + ".LOAD_TIME, Bytes.toBytes(ds.substring(11,13)+\"\"+ds.substring(14,16)+\"\"+ds.substring(17,19)))" + "\n");
	   	sb.append("\t\t\t\t.add(" + family + ", " + entityName + ".LOAD_TIMESTAMP, Bytes.toBytes(ds))" + "\n");
	   	 
	   	 sb.append("\t\t\t); \n ");
	   	 System.out.println(sb.toString());
	   	 
	   	//try's }
	   	System.out.println("\t\t}");
	   	System.out.println("\t\tcatch(Exception e)");
	   	System.out.println("\t\t{");
	   	System.out.println("\t\t\tlog.error(\"" + oldTableName.replaceAll("_", "") + " put error, msg is 【\" + msg.toString() + \"】\", e.getMessage());");
	   	System.out.println("\t\t\te.printStackTrace();");
	   	System.out.println("\t\t\ttry");
	   	System.out.println("\t\t\t{");
	   	System.out.println("\t\t\t\tmsgQueue.put(msg);");
	   	System.out.println("\t\t\t}");
	   	System.out.println("\t\t\tcatch (InterruptedException e1)");
	   	System.out.println("\t\t\t{");
	   	System.out.println("\t\t\t\tlog.error(\"" + oldTableName.replaceAll("_", "") + " put rollback error, msg is 【\" + msg.toString() + \"】\", e1.getMessage());");
	   	System.out.println("\t\t\t\te1.printStackTrace();");
	   	System.out.println("\t\t\t}");
	   	
	   	//catch's }
	   	System.out.println("\t\t}");
	   	//for's }
	   	System.out.println("\t}");
	   	System.out.println("\ttry");
	   	System.out.println("\t{");
	   	System.out.println("\t\t" + oldTableName + ".flushCommits();");
	   	System.out.println("\t\tlog.info(\"" + oldTableName + " table flush\");");
	   	System.out.println("\t}");
	   	System.out.println("\tcatch (Exception e) ");
	   	System.out.println("\t{");
	   	System.out.println("\t\tlog.error(\"" + oldTableName + " flush error\", e);");
	   	System.out.println("\t\te.printStackTrace();");
	   	System.out.println("\t\tfor (EMS_HTTP_MSG_Protos.EMS_HTTP_MSG msg : msgArr)");
	   	System.out.println("\t\t{");
	   	System.out.println("\t\t\ttry ");
	   	System.out.println("\t\t\t{");
	   	System.out.println("\t\t\t\tmsgQueue.put(msg);");
	   	System.out.println("\t\t\t}");
	   	System.out.println("\t\t\tcatch (InterruptedException e1) ");
	   	System.out.println("\t\t\t{");
	   	System.out.println("\t\t\t\tlog.error(\"" + oldTableName + " flush rollback error, msg is 【\" + msg.toString() + \"】\", e1.getMessage());");
	   	System.out.println("\t\t\t\te1.printStackTrace();");
	   	System.out.println("\t\t\t}");
	   	System.out.println("\t\t}");
	   	System.out.println("\t}");
	   	System.out.println("}\n\n");
	}

	private static void genLSHbaseInput(File f) throws Exception
	{
		InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		
		String oldTableName = f.getName().toLowerCase().replaceAll(".txt", "").toUpperCase();
		String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
		
		String family = "f";
		String entityName = "EMS_Field_List." + tableName;
		String rowkeys = "";
//    	 String v = "msg.get" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1, tableName.length()).toLowerCase() + "().get";
		String v = "msg.get" + tableName.substring(0, 1) + tableName.substring(1, tableName.length()).toLowerCase() + "().get";
		StringBuffer sb = new StringBuffer("\t\t\t" + oldTableName + ".put(new Put(rowkey.getBytes())\n");
		
		while ((line = br.readLine()) != null) 
		{
			String colName = line.substring(line.indexOf("      ")+6).split(" ")[0].toUpperCase();
			
			if ("DISTCD".equals(colName))
				sb.append("\t\t\t\t.add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(distCd))" + "\n");
			else if ("DATADT".equals(colName))
				sb.append("\t\t\t\t.add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(dataDt))" + "\n");
			else if ("LOADDT".equals(colName))
				sb.append("\t\t\t\t.add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(ds.substring(0,4)+\"\"+ds.substring(5,7)+\"\"+ds.substring(8,10)))" + "\n");
			else if ("DFILE_NAME".equals(colName))
				sb.append("\t\t\t\t.add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(line.getFileName()))" + "\n");
			else
				sb.append("\t\t\t\t.add(" + family + ", " + entityName + "." + colName + ", Bytes.toBytes(" + v + tranCol(colName).toUpperCase() + "()))" + "\n");
			
			rowkeys += (v + tranCol(colName).toUpperCase() + "()") + " + \"~\" + \n \t\t\t\t";
		}
		
		System.out.println("public void put" + oldTableName + "2HyperBase(List<LineEntity> msgArr, BlockingQueue<LineEntity> msgQueue)");
		System.out.println("{");
		System.out.println("\tfor (LineEntity line : msgArr)");
		System.out.println("\t{");
		System.out.println("\t\tEMS_FILE_MSG_Protos.EMS_FILE_MSG msg = line.getEsbMsg();\n");
		System.out.println("\t\ttry");
		System.out.println("\t\t{");
		
//		String[] fileNameSplit = line.getFileName().split("_");
//		String dataDt = fileNameSplit[5];
//		String distCd = fileNameSplit[2];
		System.out.println("\t\t\tString[] fileNameSplit = line.getFileName().split(\"_\");");
		System.out.println("\t\t\tString dataDt = fileNameSplit[5];");
		System.out.println("\t\t\tString distCd = fileNameSplit[2];");
		System.out.println("\t\t\tString czfs = fileNameSplit[6].toUpperCase();");

//		if ("I".equals(czfs))
//			putLS_DEL_LOGHyperBase(line);
		
		
		System.out.println("\t\t\tif (\"I\".equals(czfs))");
		System.out.println("\t\t\t\tputLS_DEL_LOGHyperBase(line);\n\n");
		
		System.out.println("\t\t\tString rowkey = UUID.randomUUID().toString().replaceAll(\"-\", \"\"); \n\n");
		System.out.println("\t\t\tDate d = new Date();");
		System.out.println("\t\t\tDateFormat df = new SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss\");");
		System.out.println("\t\t\tString ds = df.format(d);");
		
		
		/*sb.append("\t\t\t\t.add(" + family + ", " + entityName + ".REC_AVAIL_FLAG, REC_AVAILF_FLAG_VALID)" + "\n");
		sb.append("\t\t\t\t.add(" + family + ", " + entityName + ".LOAD_DATE, Bytes.toBytes(ds.substring(0,4)+\"\"+ds.substring(5,7)+\"\"+ds.substring(8,10)))" + "\n");
		sb.append("\t\t\t\t.add(" + family + ", " + entityName + ".LOAD_TIME, Bytes.toBytes(ds.substring(11,13)+\"\"+ds.substring(14,16)+\"\"+ds.substring(17,19)))" + "\n");
		sb.append("\t\t\t\t.add(" + family + ", " + entityName + ".LOAD_TIMESTAMP, Bytes.toBytes(ds))" + "\n");*/
		
		sb.append("\t\t\t); \n ");
		System.out.println(sb.toString());
		
		//try's }
		System.out.println("\t\t}");
		System.out.println("\t\tcatch(Exception e)");
		System.out.println("\t\t{");
		System.out.println("\t\t\tlog.error(\"" + oldTableName.replaceAll("_", "") + " put error, msg is 【\" + msg.toString() + \"】\", e.getMessage());");
		System.out.println("\t\t\te.printStackTrace();");
		System.out.println("\t\t\ttry");
		System.out.println("\t\t\t{");
		System.out.println("\t\t\t\tmsgQueue.put(line);");
		System.out.println("\t\t\t}");
		System.out.println("\t\t\tcatch (InterruptedException e1)");
		System.out.println("\t\t\t{");
		System.out.println("\t\t\t\tlog.error(\"" + oldTableName.replaceAll("_", "") + " put rollback error, msg is 【\" + msg.toString() + \"】\", e1.getMessage());");
		System.out.println("\t\t\t\te1.printStackTrace();");
		System.out.println("\t\t\t}");
		
		//catch's }
		System.out.println("\t\t}");
		//for's }
		System.out.println("\t}");
		System.out.println("\ttry");
		System.out.println("\t{");
		System.out.println("\t\t" + oldTableName + ".flushCommits();");
		System.out.println("\t\tlog.info(\"" + oldTableName + " table flush\");");
		System.out.println("\t}");
		System.out.println("\tcatch (Exception e) ");
		System.out.println("\t{");
		System.out.println("\t\tlog.error(\"" + oldTableName + " flush error\", e);");
		System.out.println("\t\te.printStackTrace();");
		System.out.println("\t\tfor (LineEntity msg : msgArr)");
		System.out.println("\t\t{");
		System.out.println("\t\t\ttry ");
		System.out.println("\t\t\t{");
		System.out.println("\t\t\t\tmsgQueue.put(msg);");
		System.out.println("\t\t\t}");
		System.out.println("\t\t\tcatch (InterruptedException e1) ");
		System.out.println("\t\t\t{");
		System.out.println("\t\t\t\tlog.error(\"" + oldTableName + " flush rollback error, msg is 【\" + msg.toString() + \"】\", e1.getMessage());");
		System.out.println("\t\t\t\te1.printStackTrace();");
		System.out.println("\t\t\t}");
		System.out.println("\t\t}");
		System.out.println("\t}");
		System.out.println("}\n\n");
	}



	private static void genJavaStatic(File f)  throws Exception
	{
		InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
	   	 BufferedReader br = new BufferedReader(isr);
	   	 String line = "";
		 String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
	   	
	   	 StringBuffer sb = new StringBuffer("public static class " + tableName + " { \n");
	   	 
	   	 while ((line = br.readLine()) != null) 
	   	 {
	   		String colName = line.substring(line.indexOf("      ")+6).split(" ")[0].toUpperCase();
	   		sb.append("	public static final byte[] " + colName + " = \"" + colName +"\".getBytes(); \n");
	   	 }
	   	 
	   	sb.append("\tpublic static final byte[] REC_AVAIL_FLAG = \"REC_AVAIL_FLAG\".getBytes(); \n");
	   	sb.append("\tpublic static final byte[] LOAD_DATE = \"LOAD_DATE\".getBytes(); \n");
	   	sb.append("\tpublic static final byte[] LOAD_TIME = \"LOAD_TIME\".getBytes(); \n");
	   	sb.append("\tpublic static final byte[] LOAD_TIMESTAMP = \"LOAD_TIMESTAMP\".getBytes(); \n");
	   	 
	   	 sb.append("} \n");
	   	 sb.append("public static class " + tableName + "_List{} \n\n");
	   	 
	   	 System.out.println(sb.toString());
	}


	private static void genProtobuf(File f) throws Exception
	{
		 InputStreamReader isr = new InputStreamReader(new FileInputStream(f), "GBK");
	   	 BufferedReader br = new BufferedReader(isr);
	   	 String line = "";
		 String tableName = transTableName(f.getName().toLowerCase().replaceAll(".txt", ""));
	   	 int i = 0;
	   	 
	   	 StringBuffer sb = new StringBuffer("message " + tableName + " { \n");
	   	 
	   	 while ((line = br.readLine()) != null) 
	   	 {
	   		 String[] cols = line.substring(line.indexOf("      ")+6).split(" ");
/*	   		 if ("DISTCD".equals(cols[0].toUpperCase()) 
	   				|| "DATADT".equals(cols[0].toUpperCase()) 
	   				|| "LOADDT".equals(cols[0].toUpperCase())
	   				|| "DFILE_NAME".equals(cols[0].toUpperCase()))
	   			 continue;
*/	   		 sb.append("\toptional string " + cols[0].toUpperCase() + " = " + (++i) + "; \n");
	   	 }
	   	 
	   	 sb.append("} \n");
	   	 
	   	 sb.append("message " + tableName + "_LIST { \n");
	   	 sb.append("	repeated " + tableName + " " + tableName.toLowerCase() + "list = 1; \n");
	   	 sb.append("} \n");
	   	 
	   	 System.out.println(sb.toString());
	}
	
	/**
	 * 
	 * @Title: Test13
	 * @Description: TODO
	 * @author: Administrator
	 * @param colName
	 * @return
	 * @throws: 
	 */
	private static String tranCol(String colName) 
	{
		String res = "";
		if (colName.indexOf("_") != -1)
		{
			String[] tempStrs = colName.split("_");
			res = trans(tempStrs);
		}
		else
		{
			res = colName.substring(0, 1).toUpperCase() + colName.substring(1, colName.length()).toLowerCase();
		}
		
		return res;
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
	private static String transTableName(String fileName) 
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
				String[] dest = new String[tempStrs.length];
				System.arraycopy(tempStrs, 0, dest, 0, tempStrs.length);
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
