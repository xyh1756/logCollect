package com.java.logCollect.collect;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailLog extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(TailLog.class);
	private final BlockingQueue<String> queue;
	private RandomAccessFile raf;
	private long lastSize = 0L;

	public TailLog(BlockingQueue<String> queue, String logName) {
		this.queue = queue;
		try {
			this.raf = new RandomAccessFile(logName, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		FileChannel channel = this.raf.getChannel();
		try {
			while (true) {
				while(this.raf.length() <= this.lastSize) {
					sleep(300);
				}

				MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, this.lastSize, this.raf.length() - this.lastSize);
				StringBuilder line = new StringBuilder();

				while(buffer.remaining() > 0) {
					line.append((char)buffer.get());
				}

				this.queue.put(line.toString());
				this.lastSize = this.raf.length();
			}
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}
}
