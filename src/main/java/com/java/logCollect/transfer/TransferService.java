package com.java.logCollect.transfer;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.net.URL;
import java.util.*;

/**
 * @author Ryan X,
 * @date 2021/12/24
 */
public class TransferService {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = readFromKafka(env);
        stream.print();
        writeToElastic(stream);
        // execute program
        env.execute("Viper Flink!");
    }

    public static DataStream<String> readFromKafka(StreamExecutionEnvironment env) {
        env.enableCheckpointing(5000);
        // set up the execution environment
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9097,localhost:9098,localhost:9099");
        properties.setProperty("group.id", "recsys");

        return env.addSource(
                new FlinkKafkaConsumer09<>("recsys", new SimpleStringSchema(), properties));
    }

    public static void writeToElastic(DataStream<String> input) {
        try {
            // Add elasticsearch hosts on startup
            List<HttpHost> transports = new ArrayList<>();
            URL url = new URL("http://127.0.0.1:9200");
            transports.add(new HttpHost(url.getHost(), url.getPort()));

            ElasticsearchSinkFunction<String> indexLog = new ElasticsearchSinkFunction<String>() {
                public IndexRequest createIndexRequest(String element) {
                    Map<String, String> esJson = new HashMap<>();
                    esJson.put("data", element);

                    return Requests
                            .indexRequest()
                            .index("log-collect")
                            .type("test-log")
                            .source(esJson);
                }

                @Override
                public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            };

            ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(transports, indexLog);
            esSinkBuilder.setRestClientFactory(
                    restClientBuilder -> restClientBuilder.setHttpClientConfigCallback(builder -> {
                        CredentialsProvider provider = new BasicCredentialsProvider();
                        provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "123456"));
                        return builder.setDefaultCredentialsProvider(provider);
                    })
            );
            input.addSink(esSinkBuilder.build());
        } catch (Exception e) {
            System.out.println("error:" + e);
        }
    }
}
