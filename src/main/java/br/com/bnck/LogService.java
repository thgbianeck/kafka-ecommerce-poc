package br.com.bnck;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

import static br.com.bnck.Constants.*;

public class LogService {
    public static void main(String[] args) {
        
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Pattern.compile(TOPICO_ALL_ECOMMERCE));

        while (true){

            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                records.forEach(rec -> {
                    System.out.println("---------------------------------------");
                    System.out.println("LOG " + rec.topic());
                    System.out.println("Key: " + rec.key());
                    System.out.println("Value: " + rec.value());
                    System.out.println("Partition: " + rec.partition());
                    System.out.println("Offset: " + rec.offset());
                    System.out.println("---------------------------------------");

                });
            }

        }

    }

    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());

        return properties;
    }
}
