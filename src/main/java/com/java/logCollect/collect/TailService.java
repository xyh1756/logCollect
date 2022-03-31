package com.java.logCollect.collect;

import java.util.*;
import java.util.concurrent.*;

import com.java.logCollect.model.TaskEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailService {

	private final static Logger logger = LoggerFactory.getLogger(TailService.class);
	private final static Map<TaskEntry, TailTask> tailTaskMap = new HashMap<>();
	private final static Map<String, MsgSender> senderTaskMap = new HashMap<>();
	private final static Set<TaskEntry> confSet = new HashSet<>();
	private final static ExecutorService tailTaskThreadPoolExecutor = new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
	private final static ExecutorService senderTaskThreadPoolExecutor = new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
	public final static Map<String, BlockingQueue<String>> queueMap = new HashMap<>();

	public static void main(String[] args) throws InterruptedException {
		confSet.add(new TaskEntry("test.log", "nginx"));
		confSet.add(new TaskEntry("test2.log", "spring"));

		for (TaskEntry taskEntry : confSet) {
			if (!tailTaskMap.containsKey(taskEntry)) {
				if (!queueMap.containsKey(taskEntry.topic)) {
					BlockingQueue<String> queue = new ArrayBlockingQueue<>(10000);
					MsgSender senderTask = new MsgSender(taskEntry.topic, queue);
					queueMap.put(taskEntry.topic, queue);
					senderTaskMap.put(taskEntry.topic, senderTask);
					logger.info("提交日志传输任务：{}", taskEntry.topic);
					senderTaskThreadPoolExecutor.execute(senderTask);
				}
				TailTask tailTask = new TailTask(taskEntry);
				tailTaskMap.put(taskEntry, tailTask);
				logger.info("提交日志收集任务：{}", taskEntry);
				tailTaskThreadPoolExecutor.execute(tailTask);
			}
		}
		Thread.sleep(2000);
		confSet.remove(new TaskEntry("test2.log", "spring"));
		for (Map.Entry<TaskEntry, TailTask> entry : tailTaskMap.entrySet()) {
			if (!confSet.contains(entry.getKey())) {
				entry.getValue().running = false;
				logger.info("关闭日志收集任务：{}", entry.getKey());
			}
		}
		for (Map.Entry<String, MsgSender> entry : senderTaskMap.entrySet()) {
			boolean exist = false;
			for (TaskEntry taskEntry : confSet) {
				if (taskEntry.topic.equals(entry.getKey())) {
					exist = true;
					break;
				}
			}
			if (!exist) {
				entry.getValue().running = false;
				logger.info("关闭日志传输任务：{}", entry.getKey());
			}
		}

	}

}
