package com.java.logCollect.dao;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.KvStoreClient;
import com.ibm.etcd.client.kv.KvClient;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Ryan X
 * @date 2022/04/01
 */
@Component
public class EtcdClient implements ConfigCenter{

    private final KvClient kvClient;

    public EtcdClient(String endPoints, int port) {
        KvStoreClient kvStoreClient = com.ibm.etcd.client.EtcdClient.forEndpoint(endPoints, port).withPlainText().build();
        this.kvClient = kvStoreClient.getKvClient();
    }

    @Override
    public List<KeyValue> getPrefix(String key) {
        RangeResponse rangeResponse = kvClient.get(ByteString.copyFromUtf8(key)).asPrefix().sync();
        return rangeResponse.getKvsList();
    }

    @Override
    public KvClient.WatchIterator watchPrefix(String key) {
        return kvClient.watch(ByteString.copyFromUtf8(key)).prevKv().asPrefix().start();
    }
}
