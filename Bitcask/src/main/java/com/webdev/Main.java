package com.webdev;

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

        try {
            Bitcask bitcask = new Bitcask();
            String key  = "station-1";
            String value = "s3dola";

            byte [] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

            bitcask.put(keyBytes, valueBytes);

            try{
                byte[] valBytesRet = bitcask.get(keyBytes);
                String valRet = new String(valBytesRet, StandardCharsets.UTF_8);
                System.out.println(valRet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            bitcask.put(keyBytes, "aizen".getBytes(StandardCharsets.UTF_8));
            try{
                byte[] valBytesRet = bitcask.get(keyBytes);
                String valRet = new String(valBytesRet, StandardCharsets.UTF_8);
                System.out.println(valRet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}