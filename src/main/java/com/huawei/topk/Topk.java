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
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
			
			// 使用线程池计算
			ExecutorService fixedThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			
			final CountDownLatch countDownLatch = new CountDownLatch(splitCount);
			Vector<MinHeap> heapList = new Vector<MinHeap>();
			for (int i = 0; i < splitCount; i++) {
				fixedThreadPool.execute(new CalcRunnable(fileEntrys.get(i)) {
					@Override
					public void run() {
						try {
							Map<String, Integer> result = count(this.getFileEntry());
							heapList.add(new MinHeap(result, k));
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							countDownLatch.countDown();
						}
						
					}
					
				});
			}
			
			try {
	            countDownLatch.await();
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        }
			fixedThreadPool.shutdown();
			
//			List<MinHeap> heapList = new ArrayList<MinHeap>();
//			for (int i = 0; i < splitCount; i++) {
//				Map<String, Integer> result = count(fileEntrys.get(i));
//				heapList.add(new MinHeap(result, k));
//			}
			
			for (int i = 0; i < heapList.size(); i++) {
				while (!heapList.get(i).getMinHeap().isEmpty()) {
		            topKHeap.add(heapList.get(i).getMinHeap().poll());
		        }
			}
			
		}
		
		ArrayList<UrlEntry> topUrls = new ArrayList<UrlEntry>();
		while (!topKHeap.getMinHeap().isEmpty()) {
			UrlEntry item = topKHeap.getMinHeap().poll();
			topUrls.add(item);
		}
		Collections.reverse(topUrls);
		
		end = System.currentTimeMillis();
		
		System.out.println("calculate topK used time:" + (end - start) + "ms");
		
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
	

}

abstract class CalcRunnable implements Runnable {
	
	private FileEntry fileEntry;
	public CalcRunnable(FileEntry entry) {
		this.fileEntry = entry;
	}

	public FileEntry getFileEntry() {
		return fileEntry;
	}
}

class MinHeap {
	private PriorityQueue<UrlEntry> minHeap;
	
	private int maxSize;
	
	public MinHeap(int k) {
		this.maxSize = k;
		minHeap = new PriorityQueue<UrlEntry>(new Comparator<UrlEntry>(){
		    public int compare(UrlEntry item1,UrlEntry item2){
		        return item1.count-item2.count;
		    }
		});
	}
	
	public MinHeap(Map<String, Integer> map, int k) {
		this(k);
		
		for (Entry<String, Integer> ele : map.entrySet()) {
			this.add(new UrlEntry(ele.getKey(), ele.getValue()));
		}
	}
	
	public PriorityQueue<UrlEntry> getMinHeap() {
		return minHeap;
	}
	
	public void add(UrlEntry urlEntry) {
		if (minHeap.size() < maxSize) {
			minHeap.add(urlEntry);	
		} else {
			UrlEntry min = minHeap.peek();
            if (min != null && urlEntry.count > min.count) {
            	minHeap.poll();
            	minHeap.add(urlEntry);
            }
		}
		
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
