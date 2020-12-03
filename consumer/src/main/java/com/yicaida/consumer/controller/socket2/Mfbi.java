package com.yicaida.consumer.controller.socket2;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Mfbi {

    public static void main(String[] args) {
        //客户端
        //1、创建客户端Socket，指定服务器地址和端口
        buildConnectAndSendMsg();
    }
    public static void buildConnectAndSendMsg(){
        try {
            Socket socket = new Socket("255.255.255.255",10086);
            sendMsg(socket);
        } catch (ConnectException ignored) {
            try {
                Thread.sleep(2000);
                System.out.println("未连接到sso，休眠两秒");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            buildConnectAndSendMsg();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void sendMsg(Socket socket)  throws IOException {
        //2、获取输出流，向服务器端发送信息
        OutputStream os = socket.getOutputStream();//字节输出流
        PrintWriter pw = new PrintWriter(os);//将输出流包装成打印流
        pw.write("ZK");
        pw.flush();
        socket.shutdownOutput();
        //3、获取输入流，并读取服务器端的响应信息
        InputStream is = socket.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String info = null;
        while((info=br.readLine())!= null){
            System.out.println("我是客户端，服务器说："+info);
        }
        //4、关闭资源
        br.close();
        is.close();
        pw.close();
        os.close();
        socket.close();
    }
//    ConnectException
}
