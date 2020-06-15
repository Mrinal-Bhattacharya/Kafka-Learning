package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getName());
        String bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //send data
        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key is " + key);
            //Key is id_0 Partition: 1
            //Key is id_1 Partition: 0
            //Key is id_2 Partition: 2
            //Key is id_3 Partition: 0
            //Key is id_4 Partition: 2
            //Key is id_5 Partition: 2
            //Key is id_6 Partition: 0
            //Key is id_7 Partition: 2
            //Key is id_8 Partition: 1
            //Key is id_9 Partition: 2

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Recieved new metadata. \n" + "Topic: " + metadata.topic() + "\n" + "Partition: " + metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: " + metadata.timestamp());

                    }
                    else {
                        logger.error("Error in producing data", exception);
                    }
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
