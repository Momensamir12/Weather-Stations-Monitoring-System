package com.webdev.server;

import com.webdev.Bitcask;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BitcaskServer {
    private final int port;
    private final int poolThreadNumber;
    private final CommandExecutor commandExecutor;
    private volatile ServerSocket serverSocket;
    private volatile ExecutorService pool;
    private volatile boolean running = false;

    public BitcaskServer(int port, int poolThreadNumber, Bitcask bitcask) {
        this.port = port;
        this.poolThreadNumber = poolThreadNumber;
        this.commandExecutor = new CommandExecutor(bitcask);
    }

    public void start() throws IOException {
        pool = Executors.newFixedThreadPool(poolThreadNumber);
        serverSocket = new ServerSocket(port);
        running = true;

        try {
            while (running) {
                Socket client;
                try {
                    client = serverSocket.accept();
                } catch (IOException e) {
                    if (!running) break; // stop() closed the socket to unblock accept()
                    throw e;
                }
                pool.submit(() -> handle(client));
            }
        } finally {
            pool.shutdown();
        }
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    void handle(Socket client) {
        try (Socket c = client;
             DataInputStream in = new DataInputStream(new BufferedInputStream(c.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(c.getOutputStream()))) {

            String command = Protocol.readString(in);
            commandExecutor.dispatch(command, in, out);
            out.flush();

        } catch (IOException e) {
            // one bad/dropped connection shouldn't take the whole server down
            System.err.println("Error handling client " + client.getRemoteSocketAddress() + ": " + e.getMessage());
        }
    }
}