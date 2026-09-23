package com.webdev;

import com.webdev.server.BitcaskServer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws IOException {

        Bitcask bitcask = new Bitcask("/home/as/WeatherStationMonitoringSystem/data/bitcask");
        BitcaskServer server = new BitcaskServer(9090, 2, bitcask);
        server.start();
    }
}