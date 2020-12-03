package com.yicaida.consumer.controller.socket;

import java.net.*;

public class Test2 {
    public static void main(String[] args) throws UnknownHostException, SocketException {
        Network2 network2 = new Network2(25341);
        network2.listen();
        Network1.sendMessage("25341",  "ZK", "25340");
    }
}
