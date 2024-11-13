package eventStreaming.cdcToKafka;

import kafka.KafkaProps;
import org.apache.kafka.clients.producer.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CdcToKafkaProducer {
    private static final Logger logger = LogManager.getLogger(CdcToKafkaProducer.class);

    private static final String JSONL_FILE_PATH = "eventStreaming/cdcToKafka/sample/stream.jsonl";
    private static String KAFKA_CONFIG_PATH = "eventStreaming/kafka.properties";

    public static void main(String[] args) {
        KafkaProps kafkaProps = new KafkaProps(KAFKA_CONFIG_PATH);

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps.getKafkaProducerProperties());
        ObjectMapper objectMapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(JSONL_FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    Object cdcEvent = objectMapper.readValue(line, Object.class);
                    String message = objectMapper.writeValueAsString(cdcEvent);

                    // Produce message to kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(kafkaProps.getKafkaTopic(), null, message);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error sending message to Kafka topic: {}", record.topic(), exception);
                        } else {
                            logger.info("Message sent successfully to topic: " + metadata.topic() + " with offset: " + metadata.offset());
                        }
                    });

                } catch (IOException e) {
                    logger.error("Error parsing line as JSON: {}", line, e);
                }
            }
        } catch (IOException e) {
            logger.error("Error reading file: {}", JSONL_FILE_PATH, e);
        } finally {
            producer.flush();
            producer.close();
        }

        logger.info("CDC Ingestion to Kafka is completed.");
    }
}
