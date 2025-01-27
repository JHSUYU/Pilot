package org.pilot;

import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

public class ShadowFileManager {
    private static final String SHADOW_DIR = "shadow";
    // Store file channels to manage them
    private static final ConcurrentHashMap<String, FileChannel> shadowChannels = new ConcurrentHashMap<>();

    public FileChannel handleFileChannel(FileChannel fileChannel) {
        try {
            // Get original file path using reflection
            String originalPath = extractPath(fileChannel);
            if (originalPath == null) {
                throw new IllegalStateException("Could not extract path from file channel");
            }

            // Create shadow path
            Path originalFilePath = Paths.get(originalPath);
            Path shadowPath = createShadowPath(originalFilePath);

            // Create shadow directory if it doesn't exist
            Files.createDirectories(shadowPath.getParent());

            // Check if shadow file exists, if not, copy the original
            if (!Files.exists(shadowPath)) {
                Files.copy(originalFilePath, shadowPath);
            }
            return getShadowFile(shadowPath);

        } catch (Exception e) {
            throw new RuntimeException("Failed to handle shadow file", e);
        }
    }

    private FileChannel getShadowFile(Path path){
        if(shadowChannels.containsKey(path.toString())){
            return shadowChannels.get(path.toString());
        }

        FileChannel shadowChannel = null;

        try{
            shadowChannel =  FileChannel.open(path,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
        }catch (Exception e){
            throw new RuntimeException("Failed to open shadow file", e);
        }

        shadowChannels.put(path.toString(), shadowChannel);
        return shadowChannel;
    }

    private String extractPath(FileChannel fileChannel) {
        try {
            for (Field field : fileChannel.getClass().getDeclaredFields()) {
                if (field.getName().equals("path")) {
                    field.setAccessible(true);
                    return (String) field.get(fileChannel);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private Path createShadowPath(Path originalPath) {
        // Create shadow path maintaining the original directory structure
        return Paths.get(originalPath.getRoot().toString(),
                SHADOW_DIR,
                originalPath.subpath(0, originalPath.getNameCount()).toString());
    }

    // Method to close and remove a shadow channel
    public void closeShadowChannel(String path) {
        FileChannel channel = shadowChannels.remove(path);
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Method to close all shadow channels
    public void closeAllShadowChannels() {
        shadowChannels.forEach((path, channel) -> {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        shadowChannels.clear();
    }

    // Get a shadow channel if it exists
    public FileChannel getShadowChannel(String path) {
        return shadowChannels.get(path);
    }
}