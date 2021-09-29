package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //create a logger for my class
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootStrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "_id " + Integer.toString(i);

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // log the key
            // id 0 is going to partition 2
            // id 1 partition 2
            // id 2 partition 2
            // id 3 partition 0
            // id 4 partition 0
            // id 5 partition 2
            // id 6 partition 0
            // id 7 partition 2
            // id 8 partition 1
            // id 9 partition 2

            // the above comment shows that id 0 is going to partition 1 and if we run the program again the output will be same for all keys as above
            // so by specifying keys we guarantee that same key always go to the same partition

            // send data -asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // this callback gets executed everytime record is successfully sent or there is an exception
                    if (exception == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partitions is: " + metadata.partition() + "\n" +
                                "Offsets: " + metadata.offset() + "\n" +
                                "TimeStamp: " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get();  // block the send to make it synchronous, dont do this in prod
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
