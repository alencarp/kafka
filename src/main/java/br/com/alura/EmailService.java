package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", emailService::parse)) {
            service.run();
        }

    }
    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("----------------------------------------------------");
        System.out.println("Sending email");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        try {
            Thread.sleep(1000); //demorar 5s entre um record e outro (só pra dizer que está processando alguma coisa)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("E-mail enviado");
    }

}
