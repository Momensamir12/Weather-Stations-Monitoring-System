package com.webdev.server;

import com.webdev.Bitcask;
import com.webdev.KeyValue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandExecutor {

    private final Map<String, CommandHandler> commands = new HashMap<>();
    private final Bitcask bitcask;

    public CommandExecutor(Bitcask bitcask) {
        this.bitcask = bitcask;

        commands.put("GET", this::handleGetCommand);
        commands.put("VIEW_ALL", this::handleViewAllCommand);
    }

    public void dispatch(String command, DataInputStream in, DataOutputStream out) throws IOException {
        CommandHandler handler = commands.get(command);

        if (handler == null) {
            throw new IOException("Unknown command: " + command);
        }

        handler.execute(in, out);
    }

    private void handleGetCommand(DataInputStream in, DataOutputStream out) throws IOException {
        byte[] key = Protocol.readBytes(in);
        byte[] value = bitcask.get(key);

        if (value == null) {
            out.writeByte(Protocol.NOT_FOUND);
            return;
        }

        out.writeByte(Protocol.FOUND);
        Protocol.writeBytes(out, value);
    }

    private void handleViewAllCommand(DataInputStream in, DataOutputStream out) throws IOException {
        List<KeyValue> all = bitcask.getAllKeysValues();

        List<KeyValue> present = new ArrayList<>(all.size());
        for (KeyValue kv : all) {
            if (kv.value() != null) {
                present.add(kv);
            }
        }

        out.writeInt(present.size());
        for (KeyValue kv : present) {
            Protocol.writeBytes(out, kv.key());
            Protocol.writeBytes(out, kv.value());
        }
    }
}