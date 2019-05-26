package com.huawei.topk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

public class Topk {
	
	public static long MAX_FILE_SIZE = 1024 * 1024 * 512L; // 512MB
	
	public static String FILE_PREFIX = "hash_";
	
	public List<UrlEntry> topk(String path, int k) {
		
		int splitCount = getHashCount(path, MAX_FILE_SIZE);
		
		System.out.println(String.format("parition hash file count = %d", splitCount));
		
		long start = System.currentTimeMillis();
		
		HashMap<Integer, FileEntry> fileEntrys = partitionByHash(path, splitCount, MAX_FILE_SIZE);
		
		long end = System.currentTimeMillis();
		
		System.out.println("parition file used time:" + (end - start) + "ms");
		
		MinHeap topKHeap = new MinHeap(k);
		
		if (fileEntrys != null) {
			
			start = System.currentTimeMillis();
			
			List<MinHeap> heapList = new ArrayList<MinHeap>();
			for (int i = 0; i < splitCount; i++) {
				Map<String, Integer> result = count(fileEntrys.get(i));
				heapList.add(new MinHeap(result, k));
			}
			
			for (int i = 0; i < heapList.size(); i++) {
				while (!heapList.get(i).getMinHeap().isEmpty()) {
		            topKHeap.add(heapList.get(i).getMinHeap().poll());
		        }
			}
			
			end = System.currentTimeMillis();
			
			System.out.println("count and build minHeap used time:" + (end - start) + "ms");
			
		}
		
		ArrayList<UrlEntry> topUrls = new ArrayList<UrlEntry>();
		while (!topKHeap.getMinHeap().isEmpty()) {
			UrlEntry item = topKHeap.getMinHeap().poll();
			topUrls.add(item);
		}
		Collections.reverse(topUrls);
		return topUrls;
		
	}
	
	private int getHashCount(String path, long maxSize) {
		
		File srcFile = new File(path);
        if (!srcFile.exists() || srcFile.isDirectory()) {
            return 0;
        }
        
        int num = Long.valueOf(srcFile.length() / maxSize).intValue() + (srcFile.length() % maxSize == 0 ? 0 : 1);
		
        return num;
	}
	
	
	public HashMap<Integer, FileEntry> partitionByHash(String path, int count, long maxSize) {
		
		File srcFile = new File(path);
        if (!srcFile.exists() || srcFile.isDirectory()) {
            return null;
        }
        
		HashMap<Integer, FileEntry> fileEntrys = new HashMap<Integer, FileEntry>();
		
		for (int i = 0; i < count; i++) {
			try {
				FileEntry entry = new FileEntry(srcFile.getParent(), FILE_PREFIX + i, maxSize);
				fileEntrys.put(i, entry);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
            
        }
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
			String lineData = null;
	        while ((lineData = reader.readLine()) != null) {
	        	int n = lineData.hashCode() % count;
	        	if (fileEntrys.containsKey(n)) {
	        		fileEntrys.get(n).writeln(lineData);
	        	}
	        }
	        
	        for (int i = 0; i < count; i++) {
                fileEntrys.get(i).bw.close();
	        }
            reader.close();
	        
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
		return fileEntrys;
	}
	
	public Map<String, Integer> count(FileEntry entry) {
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		for (int i = 0; i < entry.fileIndex + 1; i++) {
			File file = new File(entry.path,  entry.fileName + "_" + i);
			if (file.exists()) {
				try {
	                BufferedReader reader = new BufferedReader(new FileReader(file));
	                String lineData;
	                while ((lineData = reader.readLine()) != null) {
	                    if (map.containsKey(lineData)) {
	                        map.put(lineData, map.get(lineData) + 1);
	                    } else {
	                        map.put(lineData, 1);
	                    }
	                }
	                reader.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
			}
		}
		
		return map;
	}
	
	
	
	public void merge(FileEntry entry) {
		
		
		for (int i = 0; i < entry.fileIndex; i++) {
			
		}
		
	}
	

}

class MinHeap {
	private PriorityQueue<UrlEntry> minHeap;
	
	public MinHeap(int k) {
		minHeap = new PriorityQueue<UrlEntry>(k, new Comparator<UrlEntry>(){
		    public int compare(UrlEntry item1,UrlEntry item2){
		        return item1.count-item2.count;
		    }
		});
	}
	
	public MinHeap(Map<String, Integer> map, int k) {
		this(k);
		
		for (Entry<String, Integer> ele : map.entrySet()) {
			minHeap.add(new UrlEntry(ele.getKey(), ele.getValue()));
		}
	}
	
	public PriorityQueue<UrlEntry> getMinHeap() {
		return minHeap;
	}
	
	public void add(UrlEntry urlEntry) {
		minHeap.add(urlEntry);
	}
}

class UrlEntry {
	public String url;
	public int count;
	
	public UrlEntry(String url, int count) {
		this.url = url;
		this.count = count;
	}
}

class FileEntry {
	public String path;
	public String fileName;
	public long fileSize;
	public BufferedWriter bw;
	public int fileIndex;
	
	public long maxSize;
	
	public FileEntry(String path, String fileName, long maxSize) throws IOException {
		this.fileName = fileName;
		this.fileSize = 0;
		this.fileIndex = 0;		
		this.path = path;
		this.maxSize = maxSize;
		this.bw = new BufferedWriter(new FileWriter(new File(path, fileName + "_" + fileIndex)));
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}
	
	/**
	 * auto split big partition file
	 * @param line
	 * @throws IOException
	 */
	public void writeln(String line) throws IOException {
		if (fileSize > maxSize) {
			this.bw.close();
			this.bw = new BufferedWriter(new FileWriter(new File(path, fileName + "_" + (++fileIndex))));
			this.fileSize = 0;
		}
		this.fileSize += line.getBytes().length + 1;
		this.bw.write(line);
		this.bw.newLine();
	}
	
}
