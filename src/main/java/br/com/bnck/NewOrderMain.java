package br.com.bnck;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static br.com.bnck.Constants.*;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final var msg = "11111, 22222, 33333";
        final var key = msg;
        final var value = msg;
        final var email = "Thanks for your order! We are processing your order";

        var producer = new KafkaProducer<String, String>(properties());
        var record = new ProducerRecord<>(TOPICO_NEW_ORDER, key, value);

        Callback callback = (data, ex) -> {
            if (Objects.nonNull(ex)) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        producer.send(record, callback).get();

        var emailRecord = new ProducerRecord<>(TOPICO_SEND_EMAIL, email, email);
        producer.send(emailRecord, callback).get();
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}