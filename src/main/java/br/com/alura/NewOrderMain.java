package br.com.alura;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain implements Callback {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Quero produzir uma mensagem para o kafka:
        var producer = new KafkaProducer<String, String>(properties());
        for (int i = 0; i < 5; i++) {
            var key = UUID.randomUUID().toString();
            var value = key + "67523, 7894589745";
            var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", key, value);
            //ECOMMERCE_NEW_ORDER é o tópico onde vou postar uma msg
            //send() é assíncrono, para esperar terminar, dou um get(), mas assim pode dar algumas exceções. Qdo eu envio gostaria de receber uma notificação (um callback) se deu sucesso ou falha. Para o produtor de msg do kafka ficar enviando.
            // Então, colocar o callback. Daí basta implementar a interface Callback, que tem só 1 método que recebe os parãmetro de sucesso, falha. Vou fazer um lambda para isso.
            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
            };

            var email = "Thank you for your order! We are processing your order!";
            var emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", key, email);
            producer.send(record, callback).get();
            producer.send(emailRecord, callback).get();

            //envio o mesmo listener (=lambda), que imprime as msgs. Então, eu posso extrair p uma variavel.
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //onde os meus kafkas estão rodando: localhost:9092

        //transformadores de String para bytes para a CHAVE e o VALOR do KafkaProducer<String, String>
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

    }

}