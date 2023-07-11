package br.com.alura;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher implements Closeable {
    private final KafkaProducer<String, String> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());  //Quero produzir uma mensagem para o kafka:
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //onde os meus kafkas estão rodando: localhost:9092

        //transformadores de String para bytes para a CHAVE e o VALOR do KafkaProducer<String, String>
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    void send(String topic, String key, String value) throws ExecutionException, InterruptedException {

        var record = new ProducerRecord<String, String>(topic, key, value);   //ECOMMERCE_NEW_ORDER é o tópico onde vou postar uma msg.         //send() é assíncrono, para esperar terminar, dou um get(), mas assim pode dar algumas exceções. Qdo eu envio gostaria de receber uma notificação (um callback) se deu sucesso ou falha. Para o produtor de msg do kafka ficar enviando.          // Então, colocar o callback. Daí basta implementar a interface Callback, que tem só 1 método que recebe os parãmetro de sucesso, falha. Vou fazer um lambda para isso.
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
        producer.send(record, callback).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}
