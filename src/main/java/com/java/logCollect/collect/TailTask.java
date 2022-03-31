package com.java.logCollect.collect;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.java.logCollect.model.TaskEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailTask implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(TailTask.class);
	private RandomAccessFile raf;
	private long lastSize = 0L;
	private String path;
	private String topic;
	public boolean running = true;

	public TailTask(TaskEntry taskEntry) {
		this.path = taskEntry.path;
		this.topic = taskEntry.topic;
		try {
			this.raf = new RandomAccessFile(path, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		FileChannel channel = this.raf.getChannel();
		try {
			while (running) {
				while(running && this.raf.length() <= this.lastSize) {
					Thread.sleep(300);
				}

				MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, this.lastSize, this.raf.length() - this.lastSize);
				StringBuilder line = new StringBuilder();

				while(running && buffer.remaining() > 0) {
					line.append((char)buffer.get());
				}

				TailService.queueMap.get(topic).put(line.toString());
				this.lastSize = this.raf.length();
			}
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}
}
