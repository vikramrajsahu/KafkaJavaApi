/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2021 Vikramraj Sahu.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/** mvn clean compile assembly:single */

 import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CustomKafkaProducer {

    public static void Produce(String[] args) {

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
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        kafkaProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * Creating Kafka Producer
         */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProps);

        /**
         * Creating Kafka Record
         */
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);

        try {
            /** Synchronous send */
            //kafkaProducer.send(record).get();

            /** Asynchronous Send */
            kafkaProducer.send(record, new ProducerCallBack());
        } catch (Exception ex) {
            System.out.println("Something went wrong while sending the message to broker.");
            return;
        }

        //kafkaProducer.flush();
        //kafkaProducer.close();
    }


    private static class ProducerCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            Logger logger = LoggerFactory.getLogger(CustomKafkaProducer.class);
            if (e == null) {
                logger.info("\n#######################" + "\n" +
                        "## MetaData Received ##" + "\n" +
                        "## Topic: " + recordMetadata.topic().toString() + "\n" +
                        "## Partition: " + recordMetadata.partition() + "\n" +
                        "## Timestamp: " + recordMetadata.timestamp() + "\n" +
                        "## Offset: " + recordMetadata.offset() + "\n" +
                        "#######################\n"
                );
            } else {
                e.printStackTrace();
            }
        }
    }
}