package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class FraudDetectorService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER")); //Consumer, inscreva-se neste tópico
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100)); //Consumer, pergunte se tem mensagem aí dentro, por 100 milisegundos. Retorna os registros.
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                for (var record : records) {
                    System.out.println("----------------------------------------------------");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println("key: " + record.key());
                    System.out.println("value: " + record.value());
                    System.out.println("partition: " + record.partition());
                    System.out.println("offset: " + record.offset());
                    try {
                        Thread.sleep(5000); //demorar 5s entre um record e outro (só pra dizer que está processando alguma coisa)
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Order processed");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName()); //um consumer tem que estar em um grupo, aqui crio o grupo com o nome desta classe e coloco este consumer dentro.
        return properties;
    }
}
