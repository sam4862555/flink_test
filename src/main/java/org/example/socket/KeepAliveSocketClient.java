package org.example.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class KeepAliveSocketClient {
    public static void main(String[] args) {
        try {
            // 创建到 8888 端口的连接
            Socket socket = new Socket("localhost", 8099);
            System.out.println("Connected to server.");

            // 获取输出流，用于向服务器发送数据
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            // 读取键盘输入并发送到服务器
            BufferedReader keyboardReader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Type messages to send to the server. Press Ctrl+C to exit.");
            while (true) {
                String input = keyboardReader.readLine();
                out.println(input);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}