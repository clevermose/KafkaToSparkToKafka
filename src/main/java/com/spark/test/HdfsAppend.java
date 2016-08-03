package com.spark.test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsAppend {

	public static void main(String[] args) {
		
		Configuration config = new Configuration();
		FileSystem fileSystem = null; 
		
		try {
			fileSystem = FileSystem.get(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Path file = new Path("hdfs://10.249.73.142:9000/data/append.nb");
		FSDataOutputStream dos = null;
		BufferedWriter bw = null;
		
		try {
			if(fileSystem.exists(file)) {
				dos = fileSystem.append(file);
			} else {
				dos = fileSystem.create(file);
			}
			
			bw = new BufferedWriter(new OutputStreamWriter(dos));
			bw.write("wanghaisheng,28,male\n");
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(dos);
		}
		
		try {
			if(fileSystem.exists(file)) {
				dos = fileSystem.append(file);
			} else {
				dos = fileSystem.create(file);
			}
			
			bw = new BufferedWriter(new OutputStreamWriter(dos));
			bw.write("wanghaiming,31,male\n");
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(dos);
		}
		
		try {
			if(fileSystem.exists(file)) {
				dos = fileSystem.append(file);
			} else {
				dos = fileSystem.create(file);
			}
			
			bw = new BufferedWriter(new OutputStreamWriter(dos));
			bw.write("wanghaifei,33,male\n");
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(dos);
		}
		
		IOUtils.closeStream(fileSystem);
	}

}
