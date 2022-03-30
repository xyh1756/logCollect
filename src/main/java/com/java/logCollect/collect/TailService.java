package com.java.logCollect.collect;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailService {

	private final static Logger logger = LoggerFactory.getLogger(TailService.class);

	public static void main(String[] args) throws InterruptedException {
		if (args.length < 1) {
			logger.error("Usage : TailService [logfile]");
			System.exit(0);
		}

		BlockingQueue<String> queue = new ArrayBlockingQueue<String>(10000);

		for (String arg : args) {
			new TailLog(queue, arg).start();
		}

//		while (true)
//			System.out.print(queue.take());
		new MsgSender(queue).start();
	}

}
