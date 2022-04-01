package com.java.logCollect;

import com.java.logCollect.collect.TailService;
import com.java.logCollect.transfer.TransferService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


/**
 * @author Ryan X
 * @date 2022/04/01
 */
public class Main {

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(ApplicationContextConfig.class);
        TailService tailService = context.getBean(TailService.class);
        tailService.run();
        TransferService transferService = context.getBean(TransferService.class);
        transferService.run();
    }
}
