package com.stanlemon.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaStreamGroupingExample {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamGroupingExample.class);

    private static final String TOPIC_INPUT = "streams-input";
    private static final String TOPIC_OUTPUT = "streams-output";

    public static void main(String[] args) throws Exception {
        final Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-grouper");
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(TOPIC_INPUT)
            .groupByKey()
            .windowedBy(TimeWindows.of(10_000))
            .reduce((String value1, String value2) -> value1 + "|" + value2)
            .toStream()
            .map((Windowed<String> key, String value) -> new KeyValue<>(key.key(), value))
            .to(TOPIC_OUTPUT)
        ;

        // Produce some records
        new Thread(() -> {
            final Properties producerProperties = new Properties();
            producerProperties.put("bootstrap.servers", "localhost:9092");
            producerProperties.put("acks", "all");
            producerProperties.put("retries", 3);
            producerProperties.put("batch.size", 16384);
            producerProperties.put("linger.ms", 1);
            producerProperties.put("buffer.memory", 33554432);
            producerProperties.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
            producerProperties.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

            final int min = 1;
            final int max = 3;

            while (true) {
                final int key = ThreadLocalRandom.current().nextInt(min, max + 1);

                producer.send(new ProducerRecord<>(
                    TOPIC_INPUT,
                    "key" + key,
                    "record-" + System.currentTimeMillis()
                ));

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    producer.close();
                    break;
                }
            }
        }, "producer").start();

        // Consume some records
        new Thread(() -> {
            final Properties consumerProperties = new Properties();
            consumerProperties.put("bootstrap.servers", "localhost:9092");
            consumerProperties.put("group.id", "test");
            consumerProperties.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());
            consumerProperties.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());

            final KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProperties);

            consumer.subscribe(Collections.singletonList(TOPIC_OUTPUT));

            while (true) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(300);

                consumerRecords.forEach(record ->
                    logger.info(
                        "Consumer Record ({}:{}:{}) {} = {}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value()
                    )
                );

                consumer.commitAsync();

                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    consumer.close();
                    break;
                }
            }
        }, "consumer").start();

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, streamsProperties);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}
