package com.sgalvez.tools.Workers;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by sgalvez on 5/30/14.
 */
public class ConsumerQoS {

    /* final static int MSG_COUNT = 10000; */
    static boolean isRunning = true;
    public static void main(String[] args) throws IOException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("admin");
        factory.setPassword("4dm1n");
        factory.setVirtualHost("/");
        factory.setHost("localhost");
        //factory.setPort(5672);
        factory.setPort(Integer.parseInt(args[0]));

        Connection conn = null;
        try {
            conn = factory.newConnection();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        Channel channel = conn.createChannel();

        long startTime;
        long endTime;

        channel.exchangeDeclare("ha.newExchange", "direct", true);
        channel.queueDeclare("ha.newQueue", true, false, false, null);
        channel.queueBind("ha.newQueue", "ha.newExchange", "ha.newQueue");



        /*
        Handling with while
        try {
            channel.basicConsume("ha.newQueue", true, qc);
            startTime = System.currentTimeMillis();
            for (int i = 0; i < MSG_COUNT; ++i) {
                qc.nextDelivery();
            }
            endTime = System.currentTimeMillis();
            System.out.printf("Consumidos en %.3fs\n", (float)(endTime - startTime)/1000);
        } catch (Throwable e) {
            System.out.println("Errorsh!");
            System.out.print(e);
        }finally {
            channel.close();
            conn.close();
        }
        */

        boolean autoAck = false;
        int messages = 0;
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        startTime = System.currentTimeMillis();

        /*
        Basic QOS Testing
        */
        channel.basicQos(Integer.parseInt(args[1]));
        QueueingConsumer qc = new QueueingConsumer(channel);
        channel.basicConsume("ha.newQueue", autoAck, qc);

        try {
            while (true) {

                messages++;

                QueueingConsumer.Delivery delivery = qc.nextDelivery();
                String message = new String(delivery.getBody());

                System.out.println(" [x] Received '" + message + "' " + messages);
                doWork(message);
                System.out.println(" [x] Done");

                /*
                QueueingConsumer.Delivery delivery = qc.nextDelivery();
                /*
                String message = new String(delivery.getBody());
                */
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        }catch (ShutdownSignalException e) {
            System.out.println("ShutdownSignalException Captured - attempt to Reconnect");
            System.out.print(e);
        }catch (Throwable e) {
            System.out.println("Errorsh!");
            System.out.print(e);
        }finally {
            try {
                channel.close();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            conn.close();
            endTime = System.currentTimeMillis();
            System.out.printf("Consumidos: %d", messages);
            System.out.printf("Consumidos en %.3fs\n", (float)(endTime - startTime)/1000);
        }
    }

    /*
    Dummy method - built to test basic Acknowledge
     */
    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }
}
