package eventStreaming.kafkaToOpenSearch;

import kafka.KafkaProps;
import org.apache.kafka.clients.consumer.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Iterator;
import java.util.Collections;
import java.io.IOException;

public class KafkaToOpenSearch {
    private static final Logger logger = LogManager.getLogger(KafkaToOpenSearch.class);
    private static final String OPENSEARCH_HOST = "localhost";
    private static final int OPENSEARCH_PORT = 9200;

    private static String KAFKA_CONFIG_PATH = "eventStreaming/kafka.properties";
    private static RestHighLevelClient client;

    public static void main(String[] args) {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(OPENSEARCH_HOST, OPENSEARCH_PORT, "http"))
        );

        KafkaProps kafkaProps = new KafkaProps(KAFKA_CONFIG_PATH);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps.getKafkaConsumerProperties());
        consumer.subscribe(Collections.singletonList(kafkaProps.getKafkaTopic()));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(60000);
                processMessages(records);
            }
        } catch (Exception e) {
            logger.error("Exception in processing messages ", e);
        } finally {
            consumer.close();
            try {
                client.close();
            } catch (IOException e) {
                logger.error("Exception in closing client ", e);
            }
        }
    }

    // Function  to process the batch of messages
    private static void processMessages(ConsumerRecords<String, String> records) {

        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        while (iterator.hasNext()) {
            ConsumerRecord<String, String> record = iterator.next();
            String message = record.value();

            try {
                ingestToOpenSearch(message);
            } catch (IOException e) {
                logger.error("Exception while ingesting to opensearch ", e);
            }
        }
    }

    // Function to persist data to OpenSearch
    private static void ingestToOpenSearch(String message) throws IOException {

        String json = message;

        Request request = new Request("POST", "/cdc_index/_doc");
        request.setJsonEntity(json);

        try {
            Response response = client.getLowLevelClient().performRequest(request);
            logger.info("Document indexed to OpenSearch: " + response.getStatusLine());
        } catch (IOException e) {
            logger.error("Exception indexing document to OpenSearch: ", e);
        }
    }
}
