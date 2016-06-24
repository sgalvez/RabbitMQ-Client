//  The contents of this file are subject to the Mozilla Public License
//  Version 1.1 (the "License"); you may not use this file except in
//  compliance with the License. You may obtain a copy of the License
//  at http://www.mozilla.org/MPL/
//
//  Software distributed under the License is distributed on an "AS IS"
//  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
//  the License for the specific language governing rights and
//  limitations under the License.
//
//  The Original Code is RabbitMQ.
//
//  The Initial Developer of the Original Code is GoPivotal, Inc.
//  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
//

package com.sgalvez.tools.Publisher;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class SimpleProducer {
    public static void main(String[] args) {
        try {

            String amountOfMessages = (args.length > 0) ? args[0] :"100000";
            String uri = (args.length > 1) ? args[1] : "amqp://localhost";
            String message = (args.length > 2) ? args[2] :
                "the time is " + new java.util.Date().toString();
            String exchange = (args.length > 3) ? args[3] : "";
            String routingKey = (args.length > 4) ? args[4] : "ha.exampleQueue";
            String user = (args.length > 5) ? args[5] : "admin";
            String password = (args.length > 6) ? args [6] : "4dm1n";

            ConnectionFactory cfconn = new ConnectionFactory();
            cfconn.setUri(uri);
            cfconn.setUsername(user);
            cfconn.setPassword(password);
            Connection conn = cfconn.newConnection();

            Channel ch = conn.createChannel();

            if (exchange.equals("")) {
                ch.queueDeclare(routingKey, false, false, false, null);
            }

            for(int i=0; i<Integer.parseInt(amountOfMessages);i++) {
                ch.basicPublish(exchange, routingKey, null, message.getBytes());
            }
            ch.close();
            conn.close();
        } catch (Exception e) {
            System.err.println("Main thread caught exception: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
