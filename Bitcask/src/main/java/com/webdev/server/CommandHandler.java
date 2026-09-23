package com.webdev.server;

import java.io.IOException;


import java.io.DataInputStream;
import java.io.DataOutputStream;

@FunctionalInterface
interface CommandHandler {
    void execute(DataInputStream in, DataOutputStream out) throws IOException;
}