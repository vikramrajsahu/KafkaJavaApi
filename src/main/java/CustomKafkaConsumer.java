/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2021 Vikramraj Sahu.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class CustomKafkaConsumer {

    public static void Consume(String[] args) {
        if (args.length < 3) {
            System.out.println("Please enter the command in the format: <jar-file> <broker-list> <topic> <message> [<partition>]");
            return;
        }
        String bootstrapServer = args[0].toString();
        String topic = args[1].toString();
        String message = args[2].toString();

        if (bootstrapServer.isEmpty()) {
            System.out.println("Please provide Broker list.");
            return;
        }

        if (topic.isEmpty()) {
            System.out.println("Please provide topic name.");
            return;
        }

        if (message.isEmpty()) {
            System.out.println("Please provide message");
            return;
        }

        /**
         * Configuring Kafka
         */
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        kafkaProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"custom");

        /**
         * Creating Kafka Consumer
         */
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(kafkaProps);

        /**
         * Subscribe to a topic
         */
        /** Method 1 */
        //kafkaConsumer.subscribe(Collections.singleton(topic));

        /** Method 2 */
        kafkaConsumer.subscribe(Arrays.asList(topic));

        /** Method 3 */
        //kafkaConsumer.subscribe("topic.*");

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println("Record(" + consumerRecord.offset() + ")=> Key:" + consumerRecord.key() + " Value:" + consumerRecord.value());
                }
            }
        } finally {
            kafkaConsumer.close();
            System.out.println("Reading ended ...");
        }
    }
}
