package com.yicaida.consumer.controller.socket;


import java.net.*;
import java.nio.charset.StandardCharsets;


public class Network1 {

    DatagramSocket socket;

    private int ssoPortal;

    public Network1(int ssoPortal) {
        try {
            this.ssoPortal = ssoPortal;
            socket = new DatagramSocket(this.ssoPortal);
        } catch (Exception e) {
            System.err.println("Connection failed. " + e.getMessage());
        }
    }

    public void listen() {
        new Thread(() -> {
            while (true) {
                try {
                    byte[] buf = new byte[1000];
                    DatagramPacket packet = new DatagramPacket(buf,buf.length);
                    socket.receive(packet);
                    int length = 0;
                    for (int i = 0; i < buf.length; ++i) {
                        if (buf[i] == 0) {
                            length = i;
                            break;
                        }
                    }
                    String message = new String(buf, 0, length, StandardCharsets.UTF_8);
                    String rec_port = message.split(",")[0];
                    String msg = message.split(",")[1];
                    System.out.println(message);
                    System.out.println("Recieved: " + msg);
                    if ("ZK".equals(msg)) {
                        Network1.sendMessage(String.valueOf(ssoPortal), "127.0.0.1:2181", rec_port);
                    }
                    if (message.equals("end")) {
                        return;
                    }
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
        }).start();
    }

    public static void sendMessage(String fromPort, String msg, String toPort) throws SocketException, UnknownHostException {
        DatagramSocket socket = new DatagramSocket();
        String tmp = fromPort + "," +msg;
        byte[] buf= tmp.getBytes();
        DatagramPacket packet= new DatagramPacket(buf, buf.length);
        packet.setAddress(InetAddress.getByName("255.255.255.255"));
        packet.setPort(Integer.parseInt(toPort));
        try{
            socket.send(packet);
        }catch(Exception e){
            System.err.println("Sending failed. " + e.getMessage());
        }
    }
}
