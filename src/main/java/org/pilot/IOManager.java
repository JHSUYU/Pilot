package org.pilot;

import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

public class IOManager {
    private static final String SHADOW_DIR = "shadow";
    // Store file channels to manage them
    private static final ConcurrentHashMap<String, FileChannel> shadowChannels = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, FileOutputStream> shadowOutputStreams = new ConcurrentHashMap<>();

    public FileOutputStream handleFileOutputStream(FileOutputStream fileOutputStream){
        try{
            String originalPath = extractPath4FileOutputStream(fileOutputStream);
            if (originalPath == null) {
                throw new IllegalStateException("Could not extract path from file channel");
            }

            Path originalFilePath = Paths.get(originalPath);
            Path shadowPath = createShadowPath(originalFilePath);

            if(shadowOutputStreams.containsKey(shadowPath.toString())){
                return shadowOutputStreams.get(shadowPath.toString());
            }

            Files.createDirectories(shadowPath.getParent());

            if (!Files.exists(shadowPath)) {
                Files.copy(originalFilePath, shadowPath);
            }

            return getShadowFileOutputStream(shadowPath);
        }catch (Exception e){
            throw new RuntimeException("Failed to handle shadow file outputstream", e);
        }
    }

    public FileChannel handleFileChannel(FileChannel fileChannel) {
        try {
            // Get original file path using reflection
            String originalPath = extractPath4FileChannel(fileChannel);
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
            return getShadowFileChannel(shadowPath);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to handle shadow file channel", e);
        }
    }

    private FileChannel getShadowFileChannel(Path path){
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

    private FileOutputStream getShadowFileOutputStream(Path path) {
        if(shadowOutputStreams.containsKey(path.toString())){
            return shadowOutputStreams.get(path.toString());
        }

        FileOutputStream shadowOutputStream = null;

        try {
            shadowOutputStream = new FileOutputStream(path.toFile(), true);  // true表示追加模式
        } catch (Exception e) {
            throw new RuntimeException("Failed to open shadow file", e);
        }

        shadowOutputStreams.put(path.toString(), shadowOutputStream);
        return shadowOutputStream;
    }

    private String extractPath4FileChannel(FileChannel fileChannel) {
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

    private String extractPath4FileOutputStream(FileOutputStream outputStream) {
        try {
            for (Field field : outputStream.getClass().getDeclaredFields()) {
                if (field.getName().equals("path")) {
                    field.setAccessible(true);
                    return (String) field.get(outputStream);
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