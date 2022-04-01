package com.java.logCollect.collect;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgSender implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(MsgSender.class);

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

	private final BlockingQueue<String> queue;
	private final String topic;
	public boolean running = true;
	private final KafkaProducer<String, String> producer;

	public MsgSender(String topic, BlockingQueue<String> queue) {
		this.queue = queue;
		this.topic = topic;
		Properties props = new Properties();
		props.put("bootstrap.servers", "127.0.0.1:9097,127.0.0.1:9098,127.0.0.1:9099");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
	}

	@Override
	public void run() {
		while (running) {
			try {
				String data = queue.take();
				if (!data.replace("\n", "").replace("\r", "").equals("")) {
					String timestamp = sdf.format(new Date());
					for (String line : data.split("\n")) {
						ProducerRecord<String, String> record = new ProducerRecord<>(topic, timestamp, line);
						logger.info("sending line :{}, {}", topic, line);
						producer.send(record);
					}

				}
			} catch (InterruptedException e) {
				logger.error("kafka producer 消息发送失败", e);
			}
		}
	}
}