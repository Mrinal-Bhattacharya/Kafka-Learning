package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoCallBack.class.getName());
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
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world " + Integer.toString(i));
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Recieved new metadata. \n" + "Topic: " + metadata.topic() + "\n" + "Partition: " + metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: " + metadata.timestamp());

                    }
                    else {
                        logger.error("Error in producing data", exception);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
