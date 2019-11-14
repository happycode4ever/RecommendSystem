package com.jj.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.HashMap;

public class Application {
    public static void main(String[] args) {


        //生产代码，将配置抽成命令行参数
        if (args.length < 3) {
            System.err.println("Usage: kafkaStream <brokers> <zookeepers> <from> <to>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <from> is a topic to consume from\n" +
                    "  <to> is a topic to product to\n\n");
            System.exit(1);
        }
        String brokers = args[0];
        String from = args[1];
        String to = args[2];

        /*
        测试代码
        String brokers = "movie127:9092";
        String from = "log";
        String to = "recommend";
        */

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", from)
                .addProcessor("PROCESS", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESS");

        HashMap<String, String> config = new HashMap<>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        StreamsConfig streamsConfig = new StreamsConfig(config);
        KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
        streams.start();
    }
}
class LogProcessor implements Processor<byte[],byte[]>{
    ProcessorContext context = null;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        //埋点日志前缀
        String RATING_LOG_PREFIX = "movie_rating:";
        String parseValue = new String(value);
        //如果包含该埋点日志字段，认为是用户电影评分相关日志，截取后半段日志传给kafka新的topic
        if(parseValue.contains(RATING_LOG_PREFIX)){
            context.forward(key,parseValue.split(RATING_LOG_PREFIX)[1].trim().getBytes());
        }
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
