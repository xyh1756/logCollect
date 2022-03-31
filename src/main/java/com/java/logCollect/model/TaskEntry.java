package com.java.logCollect.model;

import java.util.Objects;

/**
 * @author Ryan X
 * @date 2022/03/30
 */
public class TaskEntry {
    public String path;
    public String topic;

    public TaskEntry(String path, String topic) {
        this.path = path;
        this.topic = topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskEntry taskEntry = (TaskEntry) o;
        return path.equals(taskEntry.path) && topic.equals(taskEntry.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, topic);
    }

    @Override
    public String toString() {
        return String.format("TaskEntry: {%s, %s}", path, topic);
    }
}
