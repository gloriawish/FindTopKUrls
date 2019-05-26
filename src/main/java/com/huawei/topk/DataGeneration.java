package com.huawei.topk;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class DataGeneration {
	
	private static String BASE_URL = "http://www.pingcap.com/news/";
	public void gen(String savePath, long fileSize) {

		File dir = new File(savePath);
		
		if (!dir.exists()) {
			dir.mkdirs();
		}
		
		long writeSize = 0;
		long count = 0;
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(new File(savePath, "input.txt")));
			
			while (writeSize < fileSize) {
			
				String url = BASE_URL + (System.currentTimeMillis() % 10000) + "\n";
				
				bw.write(url);
				
				writeSize += url.getBytes().length;
				
				count++;
				
				if (count != 0 && count % 10000 == 0) {
					bw.flush();
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println(String.format("generation %s success.", new File(savePath, "input.txt").getName()));
		
	}

}
