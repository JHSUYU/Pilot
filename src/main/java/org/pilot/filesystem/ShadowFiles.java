package org.pilot.filesystem;

import org.pilot.PilotUtil;
import org.pilot.State;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.pilot.PilotUtil.debug;

public class ShadowFiles {

    public static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ShadowFiles.class);


    public static boolean deleteIfExists(Path path) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.deleteIfExists(path);
        }

        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());

            Path shadowPath = ShadowFileSystem.getShadowFSPath(path.toAbsolutePath());
            Path shadowAppendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(path.toAbsolutePath());

            Files.deleteIfExists(shadowAppendLogPath);
            return Files.deleteIfExists(shadowPath);
    }


    public static boolean exists(Path path, LinkOption... options) {
        if (!PilotUtil.isDryRun()) {
            return Files.exists(path, options);
        }

        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());

        try {
            ShadowFileSystem.initializeFromOriginal();
            Path shadowPath = ShadowFileSystem.getShadowFSPath(path.toAbsolutePath());
            return Files.exists(shadowPath, options);
        } catch (IOException e) {
            LOG.warn("exists check failed", e);
            return false;
        }
    }

    public static Path walkFileTree(Path start, FileVisitor<? super Path> visitor) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.walkFileTree(start, visitor);
        }
        start = ShadowFileSystem.getOriginalFSPath(start.toAbsolutePath());

        ShadowFileSystem.initializeFromOriginal();
        Path shadowStart = ShadowFileSystem.getShadowFSPath(start.toAbsolutePath());

        // 需要包装visitor，将shadow路径转换回原始路径
        FileVisitor<Path> wrappedVisitor = new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                // 可能需要将shadow路径转换回原始路径
                return visitor.preVisitDirectory(dir, attrs);
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                return visitor.visitFile(file, attrs);
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return visitor.visitFileFailed(file, exc);
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return visitor.postVisitDirectory(dir, exc);
            }
        };

        return Files.walkFileTree(shadowStart, wrappedVisitor);
    }

    // Files.newInputStream
    public static InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.newInputStream(path, options);
        }

        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());

        ShadowFileSystem.initializeFromOriginal();
        Path absPath = path.toAbsolutePath();
        Path shadowPath = ShadowFileSystem.getShadowFSPath(absPath);
        Path shadowAppendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absPath);

        // 如果有append log，先重建
        if (Files.exists(shadowAppendLogPath) && Files.size(shadowAppendLogPath) > 0) {
            rebuildFileFromLog(absPath, shadowAppendLogPath, shadowPath);
            Files.deleteIfExists(shadowAppendLogPath);
        } else if (!Files.exists(shadowPath) && Files.exists(absPath)) {

            // 没有shadow文件但有原始文件，直接读原始文件
            return Files.newInputStream(absPath, options);
        }

        return Files.newInputStream(shadowPath, options);
    }

    public static void delete(Path path) throws IOException {
        if (debug || !PilotUtil.isDryRun()) {
            Files.delete(path);
            return;
        }

        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());
        Path shadowPath = ShadowFileSystem.getShadowFSPath(path);
        Path shadowAppendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(path);
        Files.deleteIfExists(shadowAppendLogPath);
        Files.delete(shadowPath);
        return;
    }

    //Used in FSDirecotry Solr Files.isDirectory
    public static boolean isDirectory(Path path) {
        if (!PilotUtil.isDryRun()) {
            return Files.isDirectory(path);
        }

        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());

        try {
            ShadowFileSystem.initializeFromOriginal();
        } catch (IOException e) {
            LOG.warn("isDirectory ShadowFileSystem init fails");
        }

        Path shadowPath = ShadowFileSystem.getShadowFSPath(path);
        return Files.isDirectory(shadowPath);
    }

    public static Path createDirectories(Path dir, FileAttribute<?>... attrs)
            throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.createDirectories(dir, attrs);
        }

        dir = ShadowFileSystem.getOriginalFSPath(dir.toAbsolutePath());
        try {
            ShadowFileSystem.initializeFromOriginal();
        } catch (IOException e) {
            LOG.warn("createDirectories ShadowFileSystem init fails");
        }
        Path shadowPath = ShadowFileSystem.getShadowFSPath(dir);
        return Files.createDirectories(shadowPath, attrs);
    }

    // 获取文件大小 (FSDirectory.fileLength使用)
    public static long size(Path path) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.size(path);
        }


        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());
        ShadowFileSystem.initializeFromOriginal();
        Path absPath = path.toAbsolutePath();
        Path shadowPath = ShadowFileSystem.getShadowFSPath(absPath);
        Path shadowAppendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absPath);

        if (Files.exists(shadowAppendLogPath) && Files.size(shadowAppendLogPath) > 0) {
            rebuildFileFromLog(absPath, shadowAppendLogPath, shadowPath);
            Files.deleteIfExists(shadowAppendLogPath);
            return Files.size(shadowPath);
        }

        if (Files.exists(absPath) && (Files.exists(shadowAppendLogPath) && Files.size(shadowAppendLogPath)==0)) {
            return Files.size(absPath);
        }

        return Files.size(shadowPath);
    }

    public static Path createFile(Path path, FileAttribute<?>... attrs) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.createFile(path, attrs);
        }

        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());
        ShadowFileSystem.initializeFromOriginal();
        Path absPath = path.toAbsolutePath();
        Path shadowPath = ShadowFileSystem.getShadowFSPath(absPath);

        Path parent = shadowPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        Path result = Files.createFile(shadowPath, attrs);

        Path shadowAppendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absPath);
        Files.deleteIfExists(shadowAppendLogPath);

        return result;

    }


    public static Path move(Path source, Path target, CopyOption... options) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.move(source, target, options);
        }

        source = ShadowFileSystem.getOriginalFSPath(source.toAbsolutePath());
        target = ShadowFileSystem.getOriginalFSPath(target.toAbsolutePath());
            ShadowFileSystem.initializeFromOriginal();
            Path absSource = source.toAbsolutePath();
            Path absTarget = target.toAbsolutePath();

            Path shadowSource = ShadowFileSystem.getShadowFSPath(absSource);
            Path shadowTarget = ShadowFileSystem.getShadowFSPath(absTarget);

            // 确保目标父目录存在
            Path targetParent = shadowTarget.getParent();
            if (targetParent != null && !Files.exists(targetParent)) {
                Files.createDirectories(targetParent);
            }

            Path sourceLog = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absSource);
            Path targetLog = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absTarget);

            // 检查源文件的append log
            if (Files.exists(sourceLog) && Files.size(sourceLog) > 0) {
                // 有append log，需要先重建文件
                rebuildFileFromLog(absSource, sourceLog, shadowSource);
                Files.deleteIfExists(sourceLog);
            } else if (!Files.exists(shadowSource) && Files.exists(absSource)) {
                // 没有append log，shadow文件也不存在，但原始文件存在，需要复制
                Files.copy(absSource, shadowSource);
            }

            // 删除目标的append log（如果存在）
            if (Files.exists(targetLog)) {
                Files.delete(targetLog);
            }

            // 移动shadow文件
            Path result = Files.move(shadowSource, shadowTarget, options);


            return result;
    }

    public static SeekableByteChannel newByteChannel(Path path, OpenOption... options) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.newByteChannel(path, options);
        }

        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());

        ShadowFileSystem.initializeFromOriginal();
        Path absPath = path.toAbsolutePath();
        Path shadowPath = ShadowFileSystem.getShadowFSPath(absPath);
        Path shadowAppendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absPath);

        // 确保父目录存在
        Path parent = shadowPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        // 检查是否是只读操作
        boolean isReadOnly = true;
        boolean hasCreate = false;
        for (OpenOption opt : options) {
            if (opt == StandardOpenOption.WRITE ||
                    opt == StandardOpenOption.APPEND ||
                    opt == StandardOpenOption.DELETE_ON_CLOSE) {
                isReadOnly = false;
            }
            if (opt == StandardOpenOption.CREATE ||
                    opt == StandardOpenOption.CREATE_NEW) {
                hasCreate = true;
                isReadOnly = false;
            }
        }

        // 如果append log存在且非空，需要重建
        if (Files.exists(shadowAppendLogPath) && Files.size(shadowAppendLogPath) > 0) {
            rebuildFileFromLog(absPath, shadowAppendLogPath, shadowPath);
            Files.deleteIfExists(shadowAppendLogPath);
            return Files.newByteChannel(shadowPath, options);
        }

        if (Files.exists(shadowAppendLogPath) && Files.size(shadowAppendLogPath) == 0) {
            return Files.newByteChannel(absPath, options);
        }


        // 如果shadow文件不存在
        if (!Files.exists(shadowPath)) {
            if (Files.exists(absPath)) {
                if (isReadOnly) {
                    // 只读操作，直接返回原始文件的channel
                    return Files.newByteChannel(absPath, StandardOpenOption.READ);
                } else {
                    // 需要写操作，复制原始文件到shadow
                    Files.copy(absPath, shadowPath);
                    Files.deleteIfExists(shadowAppendLogPath);

                    return Files.newByteChannel(shadowPath, options);
                }
            } else if (hasCreate) {
                // 原始文件不存在但有CREATE选项
                Files.createFile(shadowPath);
                return Files.newByteChannel(shadowPath, options);
            } else {
                // 文件不存在且没有CREATE选项，让原生方法抛出异常
                return Files.newByteChannel(absPath, options);
            }
        }

        // shadow文件存在，直接使用
        return Files.newByteChannel(shadowPath, options);
    }

    public static DirectoryStream<Path> newDirectoryStream(Path dir) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.newDirectoryStream(dir);
        }

        dir = ShadowFileSystem.getOriginalFSPath(dir.toAbsolutePath());
            ShadowFileSystem.initializeFromOriginal();
            Path shadowDir = ShadowFileSystem.getShadowFSPath(dir.toAbsolutePath());
            return Files.newDirectoryStream(shadowDir);
    }


    public static boolean delete(File file) throws IOException {
        return true;
    }

    public static boolean rename(File src, File dest) throws IOException {
        return true;
    }

    private static void rebuildFileFromLog(Path originalPath, Path appendLogPath, Path shadowPath) throws IOException {
        // 删除并重新创建shadow文件
        Files.deleteIfExists(shadowPath);
        Files.createFile(shadowPath);

        try (SeekableByteChannel rebuiltChannel = Files.newByteChannel(shadowPath,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            // 1. 复制原始文件内容（如果存在）
            if (Files.exists(originalPath)) {
                try (SeekableByteChannel origChannel = Files.newByteChannel(
                        originalPath, StandardOpenOption.READ)) {
                    ByteBuffer buffer = ByteBuffer.allocate(8192);
                    while (origChannel.read(buffer) > 0) {
                        buffer.flip();
                        rebuiltChannel.write(buffer);
                        buffer.clear();
                    }
                }
            }

            // 2. 从append log读取并应用所有写操作
            if (Files.exists(appendLogPath) && Files.size(appendLogPath) > 0) {
                List<FileOperation> operations = readOperationsFromLog(appendLogPath);

                for (FileOperation operation : operations) {
                    ByteBuffer buffer = ByteBuffer.wrap(operation.getData());
                    rebuiltChannel.position(operation.getOffset());
                    rebuiltChannel.write(buffer);
                }
            }
        }
    }

    // 辅助方法：从日志文件读取操作
    private static List<FileOperation> readOperationsFromLog(Path appendLogPath) throws IOException {
        List<FileOperation> operations = new ArrayList<>();


        if (!Files.exists(appendLogPath) || Files.size(appendLogPath) == 0) {
            return operations;
        }

        try (ObjectInputStream in = new ObjectInputStream(
                Files.newInputStream(appendLogPath))) {
            while (true) {
                try {
                    FileOperation operation = (FileOperation) in.readObject();
                    operations.add(operation);
                } catch (EOFException e) {
                    break; // 正常结束
                }
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize FileOperation", e);
        }

        PilotUtil.dryRunLog("Read " + operations.size() + " operations from append log");
        return operations;
    }

//    public static OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
//        if (!PilotUtil.isDryRun()) {
//            return Files.newOutputStream(path, options);
//        }
//
//        // 写入日志文件的辅助方法
//        writeLog("newOutputStream for path: " + path);
//
//        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());
//
//        ShadowFileSystem.initializeFromOriginal();
//        Path absPath = path.toAbsolutePath();
//        Path shadowPath = ShadowFileSystem.getShadowFSPath(absPath);
//        Path shadowAppendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absPath);
//
//        // 确保父目录存在
//        Path parent = shadowPath.getParent();
//        if (parent != null && !Files.exists(parent)) {
//            Files.createDirectories(parent);
//        }
//
//        writeLog("Shadow directory exists: " + Files.exists(Paths.get("/opt/ShadowDirectory")));
//        writeLog("Shadow append log directory exists: " + Files.exists(Paths.get("/opt/ShadowAppendLog")));
//        writeLog("Attempting to create file at: " + shadowPath.toString());
//
//        writeLog(String.format("ShadowFiles.newOutputStream for path1: %s, shadowPath: %s, shadowAppendLogPath: %s",
//                path, shadowPath, shadowAppendLogPath));
//
//        if (Files.exists(shadowAppendLogPath)) {
//            return new ShadowOutputStream(absPath, shadowAppendLogPath, shadowPath);
//        }
//
//        writeLog(String.format("ShadowFiles.newOutputStream for path2: %s, shadowPath: %s, shadowAppendLogPath: %s",
//                path, shadowPath, shadowAppendLogPath));
//
//        if(!Files.exists(shadowAppendLogPath)){
//            if(!Files.exists(shadowPath)) {
//                // 如果shadow文件不存在，创建它
//                Files.createFile(shadowPath);
//            }
//            return Files.newOutputStream(shadowPath, options);
//        }
//
//        writeLog("3");
//
//        // 如果原始文件存在但shadow文件不存在，需要决定是写append log还是复制
//        if (Files.exists(absPath) && !Files.exists(shadowPath)) {
//            // 对于原始文件，返回写入append log的流
//            return new ShadowOutputStream(absPath, shadowAppendLogPath, shadowPath);
//        }
//
//        // 其他情况（新文件或已经是shadow文件），直接写入shadow
//        writeLog("4");
//        if(!Files.exists(shadowPath)) {
//            // 如果shadow文件不存在，创建它
//            Files.createFile(shadowPath);
//        }
//
//        return Files.newOutputStream(shadowPath, options);
//    }

    public static OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.newOutputStream(path, options);
        }

        // 写入日志文件的辅助方法
        writeLog("newOutputStream for path: " + path);

        path = ShadowFileSystem.getOriginalFSPath(path.toAbsolutePath());

        ShadowFileSystem.initializeFromOriginal();
        Path absPath = path.toAbsolutePath();
        Path shadowPath = ShadowFileSystem.getShadowFSPath(absPath);
        Path shadowAppendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absPath);

        // 确保父目录存在
        Path parent = shadowPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        writeLog("Shadow directory exists: " + Files.exists(Paths.get("/opt/ShadowDirectory")));
        writeLog("Shadow append log directory exists: " + Files.exists(Paths.get("/opt/ShadowAppendLog")));
        writeLog("Attempting to create file at: " + shadowPath.toString());

        writeLog(String.format("ShadowFiles.newOutputStream for path1: %s, shadowPath: %s, shadowAppendLogPath: %s",
                path, shadowPath, shadowAppendLogPath));

        if (Files.exists(shadowAppendLogPath)) {
            return new ShadowOutputStream(absPath, shadowAppendLogPath, shadowPath);
        }

        writeLog(String.format("ShadowFiles.newOutputStream for path2: %s, shadowPath: %s, shadowAppendLogPath: %s",
                path, shadowPath, shadowAppendLogPath));

        if(!Files.exists(shadowAppendLogPath)){
            // 检查是否有创建相关的选项
            boolean hasCreateOption = false;
            boolean hasCreateNewOption = false;

            for (OpenOption option : options) {
                if (option == StandardOpenOption.CREATE) {
                    hasCreateOption = true;
                } else if (option == StandardOpenOption.CREATE_NEW) {
                    hasCreateNewOption = true;
                }
            }

            if(!Files.exists(shadowPath)) {
                // 如果shadow文件不存在，创建它
                Files.createFile(shadowPath);

                // 如果有CREATE或CREATE_NEW选项，需要过滤掉这些选项
                if (hasCreateOption || hasCreateNewOption) {
                    // 过滤掉CREATE和CREATE_NEW选项
                    List<OpenOption> filteredOptions = new ArrayList<>();
                    for (OpenOption option : options) {
                        if (option != StandardOpenOption.CREATE &&
                                option != StandardOpenOption.CREATE_NEW) {
                            filteredOptions.add(option);
                        }
                    }
                    return Files.newOutputStream(shadowPath,
                            filteredOptions.toArray(new OpenOption[0]));
                }
            }
            return Files.newOutputStream(shadowPath, options);
        }

        writeLog("3");

        // 如果原始文件存在但shadow文件不存在，需要决定是写append log还是复制
        if (Files.exists(absPath) && !Files.exists(shadowPath)) {
            // 对于原始文件，返回写入append log的流
            return new ShadowOutputStream(absPath, shadowAppendLogPath, shadowPath);
        }

        // 其他情况（新文件或已经是shadow文件），直接写入shadow
        writeLog("4");

        // 检查是否有创建相关的选项
        boolean hasCreateOption = false;
        boolean hasCreateNewOption = false;

        for (OpenOption option : options) {
            if (option == StandardOpenOption.CREATE) {
                hasCreateOption = true;
            } else if (option == StandardOpenOption.CREATE_NEW) {
                hasCreateNewOption = true;
            }
        }

        if(!Files.exists(shadowPath)) {
            // 如果shadow文件不存在，创建它
            Files.createFile(shadowPath);

            // 如果有CREATE或CREATE_NEW选项，需要过滤掉这些选项
            if (hasCreateOption || hasCreateNewOption) {
                // 过滤掉CREATE和CREATE_NEW选项
                List<OpenOption> filteredOptions = new ArrayList<>();
                for (OpenOption option : options) {
                    if (option != StandardOpenOption.CREATE &&
                            option != StandardOpenOption.CREATE_NEW) {
                        filteredOptions.add(option);
                    }
                }
                return Files.newOutputStream(shadowPath,
                        filteredOptions.toArray(new OpenOption[0]));
            }
        }

        return Files.newOutputStream(shadowPath, options);
    }

    // 添加这个辅助方法来写入日志
    private static void writeLog(String message) {
//        try (FileWriter fw = new FileWriter("/opt/log.txt", true);
//             BufferedWriter bw = new BufferedWriter(fw)) {
//            bw.write(new Date() + " - " + message);
//            bw.newLine();
//        } catch (IOException e) {
//            // 如果日志写入失败，至少打印到控制台
//            System.err.println("Failed to write log: " + e.getMessage());
//        }
    }

    // 简化ShadowOutputStream，移除对APPEND模式的特殊处理
    private static class ShadowOutputStream extends OutputStream {
        private final Path originalPath;
        private final Path appendLogPath;
        private long position = 0;
        private ObjectOutputStream logStream;
        private boolean firstWrite = true;

        public ShadowOutputStream(Path originalPath, Path appendLogPath, Path shadowPath) throws IOException {
            this.originalPath = originalPath;
            this.appendLogPath = appendLogPath;

            // 初始位置总是0，因为我们要追加写入
            this.position = 0;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[]{(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len == 0) return;

            GlobalLockManager.lock();
            try {
                byte[] data = new byte[len];
                System.arraycopy(b, off, data, 0, len);
                FileOperation operation = new FileOperation(position, data, "WRITE");

                if (firstWrite) {
                    if (!Files.exists(appendLogPath.getParent())) {
                        Files.createDirectories(appendLogPath.getParent());
                    }

                    // 检查是否已有日志文件
                    if (Files.exists(appendLogPath) && Files.size(appendLogPath) > 0) {
                        // 追加模式
                        logStream = new AppendingObjectOutputStream(
                                new FileOutputStream(appendLogPath.toFile(), true));
                    } else {
                        // 新文件
                        Files.createFile(appendLogPath);
                        logStream = new ObjectOutputStream(
                                Files.newOutputStream(appendLogPath));
                    }
                    firstWrite = false;
                }

                logStream.writeObject(operation);
                logStream.flush();
                position += len;
            } finally {
                GlobalLockManager.unlock();
            }
        }

        @Override
        public void close() throws IOException {
            if (logStream != null) {
                logStream.close();
            }
        }

        // 用于追加到已存在的ObjectOutputStream文件
        private static class AppendingObjectOutputStream extends ObjectOutputStream {
            public AppendingObjectOutputStream(OutputStream out) throws IOException {
                super(out);
            }

            @Override
            protected void writeStreamHeader() throws IOException {
                // 不写header，因为文件已经有了
            }
        }
    }
}
