package com.java.logCollect.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgSender extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(MsgSender.class);

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

	private final BlockingQueue<String> queue;
	private final KafkaProducer<String, String> producer;

	public MsgSender(BlockingQueue<String> queue) {
		this.queue = queue;

		Properties props = new Properties();
		props.put("bootstrap.servers", "127.0.0.1:9097,127.0.0.1:9098,127.0.0.1:9099");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
	}

	@Override
	public void run() {
		while (true) {
			try {
				String line = queue.take();
				if (!line.replace("\n", "").replace("\r", "").equals("")) {
					String timestamp = sdf.format(new Date());
					ProducerRecord<String, String> data = new ProducerRecord<>("recsys", timestamp, line);
					logger.debug("sending kv :({}:{})", timestamp, line);
					producer.send(data);
				}
			} catch (InterruptedException e) {
				logger.error("kafka producer 消息发送失败", e);
			}
		}
	}
}