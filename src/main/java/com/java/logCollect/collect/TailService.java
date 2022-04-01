package com.java.logCollect.collect;

import java.util.*;
import java.util.concurrent.*;

import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.WatchUpdate;
import com.ibm.etcd.api.Event;
import com.java.logCollect.dao.ConfigCenter;
import com.java.logCollect.model.TaskEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TailService {

	@Autowired
	private ConfigCenter configCenter;
	private final Logger logger = LoggerFactory.getLogger(TailService.class);
	private final static Map<TaskEntry, TailTask> tailTaskMap = new HashMap<>();
	private final static Map<String, MsgSender> senderTaskMap = new HashMap<>();
	private final static ExecutorService tailTaskThreadPoolExecutor = new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
	private final static ExecutorService senderTaskThreadPoolExecutor = new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
	public final static Map<String, BlockingQueue<String>> queueMap = new HashMap<>();

	public TailService() {

	}

	public void run() {
		List<KeyValue> configList = configCenter.getPrefix("1#");
		for (KeyValue config : configList) {
			String[] value = config.getValue().toStringUtf8().split("#");
			TaskEntry taskEntry = new TaskEntry(value[0], value[1]);
			if (!tailTaskMap.containsKey(taskEntry)) {
				commitSenderTask(taskEntry);
				commitTailTask(taskEntry);
			}
		}
		KvClient.WatchIterator watchIterator = configCenter.watchPrefix("1#");
		while (watchIterator.hasNext()) {
			WatchUpdate watchUpdate = watchIterator.next();
			List<Event> evenList = watchUpdate.getEvents();
			for (Event event : evenList) {
				if (event.getTypeValue() == 0) {
					String[] value = event.getKv().getValue().toStringUtf8().split("#");
					TaskEntry taskEntry = new TaskEntry(value[0], value[1]);
					commitSenderTask(taskEntry);
					if (event.hasPrevKv()) {
						String[] prevValue = event.getPrevKv().getValue().toStringUtf8().split("#");
						TaskEntry prevTask = new TaskEntry(prevValue[0], prevValue[1]);
						deleteTailTask(prevTask);
					}
					commitTailTask(taskEntry);
				} else if (event.getTypeValue() == 1) {
					String[] value = event.getKv().getValue().toStringUtf8().split("#");
					TaskEntry taskEntry = new TaskEntry(value[0], value[1]);
					deleteTailTask(taskEntry);
				}
			}
		}
	}

	private void commitSenderTask(TaskEntry taskEntry) {
		if (!queueMap.containsKey(taskEntry.topic)) {
			BlockingQueue<String> queue = new ArrayBlockingQueue<>(10000);
			MsgSender senderTask = new MsgSender(taskEntry.topic, queue);
			queueMap.put(taskEntry.topic, queue);
			senderTaskMap.put(taskEntry.topic, senderTask);
			logger.info("提交日志传输任务：{}", taskEntry.topic);
			senderTaskThreadPoolExecutor.execute(senderTask);
		}
	}

	private void commitTailTask(TaskEntry taskEntry) {
		TailTask tailTask = new TailTask(taskEntry);
		tailTaskMap.put(taskEntry, tailTask);
		logger.info("提交日志收集任务：{}", taskEntry);
		tailTaskThreadPoolExecutor.execute(tailTask);
	}

	private void deleteTailTask(TaskEntry taskEntry) {
		if (tailTaskMap.containsKey(taskEntry)) {
			tailTaskMap.get(taskEntry).running = false;
			tailTaskMap.remove(taskEntry);
			logger.info("删除日志收集任务：{}", taskEntry);
		}
	}
}
