package com.java.logCollect.dao;

import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.client.kv.KvClient;

import java.util.List;

/**
 * @author Ryan X
 * @date 2022/04/01
 */
public interface ConfigCenter {
    KvClient.WatchIterator watchPrefix(String key);
    List<KeyValue> getPrefix(String key);
}
