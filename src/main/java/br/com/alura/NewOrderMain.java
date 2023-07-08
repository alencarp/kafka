package br.com.alura;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain implements Callback {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Quero produzir uma mensagem para o kafka:
        var producer = new KafkaProducer<String, String>(properties());
        var value = "1234, 67523, 7894589745";
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);//ECOMMERCE_NEW_ORDER é o tópico onde vou postar uma msg

        producer.send(record, (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        }).get();
        //send() é assíncrono, para esperar terminar, dou um get(), mas assim pode dar algumas exceções.
        //qdo eu envio gostaria de receber uma notificação (um callback) se deu sucesso ou falha. Para o produtor de msg do kafka ficar enviando.

        // Então, colocar o callback. Daí basta implementar a interface Callback, que tem só 1 método que recebe os parãmetro de sucesso, falha
        //Vou fazer um lambda para isso.

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