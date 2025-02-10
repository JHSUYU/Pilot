package org.pilot.filesystem;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ShadowFileState {
    private final Path originalPath;
    private Path appendLogPath;
    public Path reconstructedPath;
    public boolean existBeforePilot;

    public ShadowFileState(Path originalPath, Path shadowRoot) {
        this.originalPath = originalPath;
    }

    public Path getOriginalPath() {
        return originalPath;
    }

    public Path getAppendLogPath() {
        return appendLogPath;
    }

    public void setAppendLogPath(Path appendLogPath) {
        this.appendLogPath = appendLogPath;
    }

    // 记录新的写操作到append log文件
    public void recordOperation(FileOperation operation) throws IOException {
        if(!Files.exists(appendLogPath)){
            Files.createFile(appendLogPath);
        }
        System.out.println("Write operation to append log: " + operation.toString());

        boolean hasHeader = Files.size(appendLogPath) > 0;
        if (hasHeader) {
            try (AppendingObjectOutputStream out = new AppendingObjectOutputStream(
                    new FileOutputStream(appendLogPath.toFile(), true))) {
                out.writeObject(operation);
            }
        } else {
            // 新文件或空文件，需要写入header
            try (ObjectOutputStream out = new ObjectOutputStream(
                    Files.newOutputStream(appendLogPath.toFile().toPath()))) {
                out.writeObject(operation);
            }
        }
    }

    public class AppendingObjectOutputStream extends ObjectOutputStream {
        public AppendingObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // 什么也不做，从而避免写入额外的流头
        }
    }

    // 从append log文件读取所有操作
    public List<FileOperation> getOperations() {
        List<FileOperation> operations = new ArrayList<>();
        if (!Files.exists(appendLogPath)) {
            return operations;
        }

        try (ObjectInputStream in = new ObjectInputStream(
                new FileInputStream(appendLogPath.toFile()))) {
            while (true) {
                try {
                    FileOperation operation = (FileOperation) in.readObject();
                    operations.add(operation);
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            // 记录日志或适当处理异常
        }
        System.out.println("Read operations from append log: " + operations.size());
        return operations;
    }
}