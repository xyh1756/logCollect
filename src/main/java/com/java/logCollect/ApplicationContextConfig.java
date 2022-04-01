package com.java.logCollect;

import com.java.logCollect.collect.TailService;
import com.java.logCollect.dao.ConfigCenter;
import com.java.logCollect.dao.EtcdClient;
import com.java.logCollect.transfer.TransferService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Ryan X
 * @date 2022/04/01
 */
@Configuration
public class ApplicationContextConfig {
    @Bean
    public ConfigCenter configCenter() {
        return new EtcdClient("127.0.0.1", 2379);
    }

    @Bean
    public TailService tailService() {
        return new TailService();
    }

    @Bean
    public TransferService transferService() {
        return new TransferService();
    }
}
