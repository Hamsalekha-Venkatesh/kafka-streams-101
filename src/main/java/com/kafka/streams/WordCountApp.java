package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // 1 - Stream from Kafka Input topic...
        KStream<String, String> wordCountInput = builder.stream("wordcount-input-topic");

        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(inputLine -> inputLine.toLowerCase()) // E.g. I/p: <Kafka Kakfa STreams> ;  o/p: <kafka kakfa streams>
                .flatMapValues(eachLine -> Arrays.asList(eachLine.split(" "))) // split the values by space
                .selectKey((ignoredKey, word) -> word) //select a key to apply
                .groupByKey() // Group similar keys to one bucket
                .count(Named.as("Counts")); // count instance of the key..

        wordCounts.toStream().to("wordcount-output-topic", Produced.with(Serdes.String(), Serdes.Long()));

        try (KafkaStreams streams = new KafkaStreams(builder.build(), properties)) {
            streams.start();

            System.out.println("Kafka Streams application started...Topology: " + streams);

            // Keep running until terminated
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down...");
                streams.close();
            }));

            // Block main thread to keep the app alive
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Application interrupted");
        }
    }
}
