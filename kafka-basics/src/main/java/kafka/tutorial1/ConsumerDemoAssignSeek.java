package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-application";
        String topic = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partition));
        long offsetToReadFrom = 15L;
        int numOfMsgsToRead = 5;
        int numOfMsgsReadSoFar = 0;
        boolean keepOnReading = true;

        consumer.seek(partition, offsetToReadFrom);
        while (keepOnReading) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                numOfMsgsReadSoFar++;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition " + record.partition() + ", Offset: " + record.offset());
                if (numOfMsgsReadSoFar >= numOfMsgsToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
