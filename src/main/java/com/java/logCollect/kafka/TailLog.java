package com.java.logCollect.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailLog extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(TailLog.class);

	private final BlockingQueue<String> queue;
	private final String logname;

	private final CharBuffer buf = CharBuffer.allocate(4096);

	// private ByteBuffer buf = ByteBuffer.allocate(4096);

	public TailLog(BlockingQueue<String> queue, String logname) {
		this.queue = queue;
		this.logname = logname;
	}

	@Override
	public void run() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(logname));

			long filesize = 0;
			while (true) {
				// 判断文件是否已经切换
				if (filesize > new File(logname).length()) {
					logger.debug("filesize :{}     current system file size :{} . Log file switchover!", filesize,
							new File(logname).length());
					try {
						// 在切换读文件前，读取文件全部内容
						StringBuilder line = new StringBuilder();
						while (reader.read(buf) > 0) {
							buf.flip();
							synchronized (buf) {
								// 读buffer 并解析
								for (int i = 0; i < buf.limit(); i++) {
									char c = buf.get();
									line.append(c);
									if ((c == '\n') || (c == '\r'))
										if (line.length() > 0) {
											queue.put(line.toString());
											line = new StringBuilder();
										}
								}
							}
						}
						queue.put(line.toString());
						buf.clear();

						// 切换读文件
						reader.close();
						reader = new BufferedReader(new FileReader(new File(logname)));
					} catch (Exception e) {
						logger.error("文件 {} 不存在", logname, e);
						sleep(10000);
						continue;
					}
				}

				for (int retrys = 10; retrys > 0; retrys--) {
					int bufread = reader.read(buf);
					if (bufread < 0) {
						sleep(1000);
					} else {
						filesize = new File(logname).length();
						retrys = -1;

						buf.flip();
						synchronized (buf) {
							// 读buffer 并解析
							StringBuilder line = new StringBuilder();
							for (int i = 0; i < buf.limit(); i++) {
								char c = buf.get();
								line.append(c);
								if ((c == '\n') || (c == '\r'))
									if (line.length() > 0) {
										queue.put(line.toString());
										line = new StringBuilder();
									}
							}
							// 接着写不完整的数据
							buf.compact();
							if (line.length() > 0) {
								buf.append(line);
							}
						}
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("文件读取失败", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.error("文件 reader 关闭失败", e);
				}
			}
		}
	}
}