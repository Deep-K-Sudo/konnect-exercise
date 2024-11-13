package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaProps {

    private Properties kafkaProperties;

    public KafkaProps(String configFile) {
        kafkaProperties =readPropFile(configFile);
    }

    public static Properties readPropFile(String configFile) {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            properties.load(fis);
            return properties;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load Kafka configuration");
        }
    }

    public Properties getKafkaProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty(KafkaConf.KAFKA_BROKER));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProperty(KafkaConf.KEY_SERIALIZER));
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProperty(KafkaConf.VALUE_SERIALIZER));

        return producerProperties;
    }

    public Properties getKafkaConsumerProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty(KafkaConf.KAFKA_BROKER));
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty(KafkaConf.GROUP_ID));
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getProperty(KafkaConf.KEY_DESERIALIZER));
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getProperty(KafkaConf.VALUE_DESERIALIZER));
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getProperty(KafkaConf.AUTO_OFFSET_RESET));

        return consumerProperties;
    }

    public String getKafkaTopic() {
        return kafkaProperties.getProperty(KafkaConf.KAFKA_TOPIC);
    }
}
