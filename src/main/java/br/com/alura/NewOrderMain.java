package br.com.alura;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 5; i++) {

                var key = UUID.randomUUID().toString();
                var value = key + "67523, 7894589745";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are processing your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);                        //envio o mesmo listener (=lambda), que imprime as msgs. Então, eu posso extrair p uma variavel.
            }
        }
    }


}