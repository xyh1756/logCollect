package com.java.logCollect.test;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.lang.Thread.sleep;

/**
 * @author Ryan X
 * @date 2022/03/30
 */
public class GenerateService {

    private RandomAccessFile raf;

    public GenerateService() {
        //    private final static Logger logger = LoggerFactory.getLogger(TailLog2.class);
        String logName = "test.log";
        try {
            this.raf = new RandomAccessFile(logName, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        FileChannel channel = raf.getChannel();
        try {
            int i = 0;
            while (true) {
                String str = "abc" + i + "\n";
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, raf.length(), str.length());
                buffer.put(str.getBytes());
//                buffer.force();
                i++;
//                sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        GenerateService generateService = new GenerateService();
        generateService.run();
    }

}
