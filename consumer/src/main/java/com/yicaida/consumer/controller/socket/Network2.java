package com.yicaida.consumer.controller.socket;


import java.net.DatagramPacket;
import java.net.DatagramSocket;


public class Network2 {

    DatagramSocket socket;

    public Network2(int server_portal) {
        try {
            socket = new DatagramSocket(server_portal);
        } catch (Exception e) {
            System.err.println("Connection failed. " + e.getMessage());
        }
    }

    public void listen() {
        new Thread() {
            public void run() {
                while (true) {
                    try {
                        byte[] buf = new byte[1000];
                        DatagramPacket packet = new DatagramPacket(buf,buf.length);
                        socket.receive(packet);
                        String message = new String(buf);
                        System.out.println("Recieved: " + message);
                        return;
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
            }
        }.start();
    }
}
