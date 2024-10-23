package org.example.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class NetCatServer {
    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(8099)) {
            System.out.println("Listening on port 8011...");
            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                     BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                    String inputLine;
                    while ((inputLine = in.readLine())!= null) {
                        System.out.println("Received: " + inputLine);
                        out.println("Echo: " + inputLine);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}