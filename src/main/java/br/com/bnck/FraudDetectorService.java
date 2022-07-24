package br.com.bnck;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static br.com.bnck.Constants.*;

public class FraudDetectorService {
    public static void main(String[] args) {
        
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList(TOPICO_NEW_ORDER));

        while (true){

            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                records.forEach(rec -> {
                    System.out.println("---------------------------------------");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println("Key: " + rec.key());
                    System.out.println("Value: " + rec.value());
                    System.out.println("Partition: " + rec.partition());
                    System.out.println("Offset: " + rec.offset());
                    System.out.println("---------------------------------------");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        //ignoring
                        throw new RuntimeException(e);
                    }

                    System.out.println("Order processed!");
                });
            }

        }

    }

    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());

        return properties;
    }
}
