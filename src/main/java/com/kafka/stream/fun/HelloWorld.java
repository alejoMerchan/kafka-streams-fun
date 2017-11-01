package com.kafka.stream.fun;




import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class HelloWorld {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"hello_world_app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(props);

        Serde<String> stringSerde = Serdes.String();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<String, String> simpleFirstStream = kStreamBuilder.stream(stringSerde,stringSerde,"src-topic");

        KStream<String,String> upperCasedStream = simpleFirstStream.mapValues(String::toUpperCase);

        upperCasedStream.to(stringSerde,stringSerde,"out-topic");

        upperCasedStream.print("hello world app");


        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,streamsConfig);
        System.out.println("Hello World App Started");
        kafkaStreams.start();
        Thread.sleep(35000);
        System.out.println("Shutting down the Hello World APP now");
        kafkaStreams.close();



    }
}
