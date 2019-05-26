package com.huawei.topk;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;

public class Main {

	public static void main(String[] args) throws Exception {

		CommandLineParser parser = new BasicParser();
		Options options = new Options();
		
		options.addOption("h", "help", false, "Print this usage information");
		
		options.addOption("g", "gen", false, "Data genration mode");
		
		options.addOption("p", "path", true, "Generate file save path");
		
		options.addOption("s", "size", true, "Generate file size");
		
		options.addOption("k", "topk", true, "The top K");
		
		options.addOption("f", "file", true, "URL file path");
		

		CommandLine commandLine = parser.parse(options, args);
		
		if (commandLine.hasOption("h")) {
			help();
			System.exit(0);
		}
		
		if (commandLine.hasOption("g")) {

			long size = 0L;
			String path = "";
			if (commandLine.hasOption('s')) {
				size = 1024 * 1024 * 1024L * Long.parseLong(commandLine.getOptionValue('s'));
			} else {
				size = 1024 * 1024 * 1024L; // 1GB
			}
			
			if (commandLine.hasOption('p')) {
				path = commandLine.getOptionValue('p');
			} else {
				path = "./data/";
			}

			DataGeneration d = new DataGeneration();
			
			d.gen(path, size);
		} else {
			String urlFile = "";
			int k = 0;
			if (commandLine.hasOption('f')) {
				urlFile = commandLine.getOptionValue('f');
			} else {
				System.out.println("-f is necessary parameter");
				help();
				System.exit(0);  
			}
			
			if (commandLine.hasOption('k')) {
				k = Integer.parseInt(commandLine.getOptionValue('k'));
			} else {
				k = 100;
			}
			
			
			if (new File(urlFile).isDirectory() || !new File(urlFile).exists()) {
				System.out.println("url file is not exists or not file.");
				System.exit(-1);
			}
			
			getTopK(urlFile, k);
		}


	}
	
	public static void help() {
		
		System.out.println("Options:");
		System.out.println("-h,--help  display help info");
		System.out.println("-g,--gen  Data genration mode");
		System.out.println("-p,--path  Generate file save path");
		System.out.println("-s,--size  Generate file size");
		System.out.println("-f,--url  URL file path");
		System.out.println("-k,--topk  The top K");
	}
	
	public static void getTopK(String urlFile, int k) {
		Topk p = new Topk();
		
		List<UrlEntry> topUrls = p.topk(urlFile, k);

		for (int i = 0; i < topUrls.size(); i++) {
			UrlEntry item = topUrls.get(i);
			System.out.println(item.url + "\t->\t" + item.count);	
		}
		
	}

}
