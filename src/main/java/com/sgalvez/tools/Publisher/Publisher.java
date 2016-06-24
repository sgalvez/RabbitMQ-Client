package com.sgalvez.tools.Publisher;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by sgalvez on 5/29/14.
 */
public class Publisher {

    final static int MSG_COUNT = 10000;
    public static void main(String [] args) throws IOException {

        ConnectionFactory factory = new ConnectionFactory();

        int amountOfMessages;
        if(args.length < 1){
            amountOfMessages = MSG_COUNT;
            factory.setHost("localhost");
            factory.setPort(5672);
        }
        else {
            amountOfMessages = Integer.parseInt(args[0]);
            factory.setHost(args[1].toString());
            factory.setPort(Integer.parseInt(args[2]));
        }


        factory.setUsername("admin");
        factory.setPassword("4dm1n");
        factory.setVirtualHost("/");


        Connection conn = null;
        try {
            conn = factory.newConnection();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        Channel channel = conn.createChannel();

        try {

            long startTime = System.currentTimeMillis();


            /*Sending Message*/
            channel.exchangeDeclare("ha.newExchange", "direct", true);
            channel.queueDeclare("ha.newQueue", true, false, false, null);
            channel.queueBind("ha.newQueue", "ha.newExchange", "ha.newQueue");


            /*
            Adding transactional confirmation to the Publisher decrease throughput by a factor of 250
            source: https://www.rabbitmq.com/confirms.html
            channel.txSelect();
            byte[] messageBodyBytes = "Hola mundo!".getBytes();
            for (int i = 0; i < MSG_COUNT; ++i) {
                channel.basicPublish("ha.newExchange", "ha.newQueue", MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
            }
            channel.txCommit();
            */

            /*
            Confirmations now are handled by confirmSelect and waitForConfirms
             */
            channel.confirmSelect();
            byte[] messageBodyBytes = "Hola mundo!".getBytes();

            for (int i = 0; i < amountOfMessages; ++i) {
                channel.basicPublish("ha.newExchange", "ha.newQueue", MessageProperties.PERSISTENT_BASIC, messageBodyBytes);

            }
            channel.waitForConfirmsOrDie();

            long endTime = System.currentTimeMillis();
            System.out.printf("Tiempo en encolar %.3fs\n", (float)(endTime - startTime)/1000);
        } catch (Throwable e) {
            System.out.println("Nos caimos... :(");
            System.out.print(e);
        }finally {
            try {
                channel.close();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            conn.close();
        }
    }



}
