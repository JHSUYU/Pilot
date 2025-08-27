package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import static org.pilot.PilotUtil.debug;

public class ShadowFileChannel extends FileChannel {
    private FileChannel delegate;
    private final Path originalPath;
    private final Path shadowPath;
    private final Path appendLogPath;
    private final OpenOption[] options;
    private long currentPosition = 0;
    private boolean isRebuilt = false;

    public ShadowFileChannel(Path originalPath, Path shadowPath, Path appendLogPath,
                             FileChannel delegate, OpenOption[] options) {
        this.originalPath = originalPath;
        this.shadowPath = shadowPath;
        this.appendLogPath = appendLogPath;
        this.delegate = delegate;
        this.options = options;

    }

    public boolean needsRebuild(){
        int size = 0;
        try{
            size = (int) Files.size(appendLogPath);
        }catch(IOException e) {
            size = 0;
        }
        return Files.exists(appendLogPath) &&
                (size > 0);
    }

    private void ensureRebuilt() throws IOException {
        if(isRebuilt) {
            return;
        }

        if (needsRebuild()) {
            rebuildFromLog();
            isRebuilt = true;
        }else{
            // 如果没有append log，直接使用delegate
            if (delegate == null ) {
                delegate = FileChannel.open(originalPath, options);
            }
        }
    }

    private void rebuildFromLog() throws IOException {
        // 重建文件
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

            // 2. 应用所有写操作
            List<FileOperation> operations = readOperationsFromLog(appendLogPath);
            for (FileOperation operation : operations) {
                ByteBuffer buffer = ByteBuffer.wrap(operation.getData());
                rebuiltChannel.position(operation.getOffset());
                rebuiltChannel.write(buffer);
            }

            currentPosition = rebuiltChannel.position();
        }

        // 重新打开delegate
        if (delegate != null) {
            delegate.close();
        }
        delegate = FileChannel.open(shadowPath, options);
        delegate.position(currentPosition);

        // 删除append log
        Files.deleteIfExists(appendLogPath);
    }

    private List<FileOperation> readOperationsFromLog(Path logPath) throws IOException {
        List<FileOperation> operations = new ArrayList<>();

        if (!Files.exists(logPath) || Files.size(logPath) == 0) {
            return operations;
        }

        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(logPath))) {
            while (true) {
                try {
                    FileOperation operation = (FileOperation) in.readObject();
                    operations.add(operation);
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize FileOperation", e);
        }

        return operations;
    }

    private void recordOperation(FileOperation operation) throws IOException {
        if (!Files.exists(appendLogPath.getParent())) {
            Files.createDirectories(appendLogPath.getParent());
        }

        if (Files.exists(appendLogPath) && Files.size(appendLogPath) > 0) {
            // 追加到现有日志
            try (AppendingObjectOutputStream out = new AppendingObjectOutputStream(
                    new FileOutputStream(appendLogPath.toFile(), true))) {
                out.writeObject(operation);
            }
        } else {
            // 创建新日志
            try (ObjectOutputStream out = new ObjectOutputStream(
                    Files.newOutputStream(appendLogPath))) {
                out.writeObject(operation);
            }
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        GlobalLockManager.lock();
        try {
            ensureRebuilt();
            return delegate.read(dst);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        GlobalLockManager.lock();
        try {
            ensureRebuilt();
            return delegate.read(dsts, offset, length);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        GlobalLockManager.lock();
        try {
            ensureRebuilt();
            return delegate.read(dst, position);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        GlobalLockManager.lock();
        try {
            // 如果有append log或原始文件存在，写入append log
            if (Files.exists(appendLogPath)) {
                byte[] data = new byte[src.remaining()];
                src.get(data);

                FileOperation operation = new FileOperation(currentPosition, data, "WRITE");
                recordOperation(operation);
                currentPosition += data.length;
                return data.length;
            } else {
                // 新文件，直接写入shadow
                return delegate.write(src);
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        GlobalLockManager.lock();
        try {
            if (Files.exists(appendLogPath)) {
                long totalLength = 0;
                for (int i = offset; i < offset + length; i++) {
                    totalLength += srcs[i].remaining();
                }

                byte[] combinedData = new byte[(int) totalLength];
                int pos = 0;
                for (int i = offset; i < offset + length; i++) {
                    ByteBuffer src = srcs[i];
                    int len = src.remaining();
                    src.get(combinedData, pos, len);
                    pos += len;
                }

                FileOperation operation = new FileOperation(currentPosition, combinedData, "WRITE");
                recordOperation(operation);
                currentPosition += totalLength;
                return totalLength;
            } else {
                return delegate.write(srcs, offset, length);
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        GlobalLockManager.lock();
        try {
            if (Files.exists(appendLogPath)) {
                byte[] data = new byte[src.remaining()];
                src.get(data);

                FileOperation operation = new FileOperation(position, data, "WRITE");
                recordOperation(operation);
                return data.length;
            } else {
                return delegate.write(src, position);
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long position() throws IOException {
        GlobalLockManager.lock();
        try {

            ensureRebuilt();
            return delegate.position();
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        GlobalLockManager.lock();
        try {
            currentPosition = newPosition;
            ensureRebuilt();
            delegate.position(newPosition);
            return this;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long size() throws IOException {
        GlobalLockManager.lock();
        try {
            if(delegate!=null){
                return delegate.size();
            }
            ensureRebuilt();
            return delegate.size();
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        GlobalLockManager.lock();
        try {
            if(delegate!=null){
                return delegate.truncate(size);
            }
            rebuildFromLog();
            isRebuilt = true;
            delegate.truncate(size);
            return this;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        GlobalLockManager.lock();
        try {
            if(delegate!=null){
                delegate.force(metaData);
                return;
            }

            rebuildFromLog();
            isRebuilt = true;
            delegate.force(metaData);

            // 如果需要重建，不执行force操作
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        GlobalLockManager.lock();
        try {
            ensureRebuilt();
            return delegate.transferTo(position, count, target);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        GlobalLockManager.lock();
        try {
            ensureRebuilt();
            return delegate.transferFrom(src, position, count);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        GlobalLockManager.lock();
        try {
            ensureRebuilt();
            return delegate.map(mode, position, size);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        GlobalLockManager.lock();
        try {
            ensureRebuilt();
            return delegate.lock(position, size, shared);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        GlobalLockManager.lock();
        try {
            ensureRebuilt();
            return delegate.tryLock(position, size, shared);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    protected void implCloseChannel() throws IOException {
        GlobalLockManager.lock();
        try {
            if (delegate != null) {
                delegate.close();
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    public static FileChannel open(Path originalPath, OpenOption... options) throws IOException {
        GlobalLockManager.lock();
        try {
            if (debug || !PilotUtil.isDryRun()) {
                return FileChannel.open(originalPath, options);
            }

            originalPath = ShadowFileSystem.getOriginalFSPath(originalPath);

            ShadowFileSystem.initializeFromOriginal();
            Path absOriginal = originalPath.toAbsolutePath();
            Path shadowPath = ShadowFileSystem.resolveShadowFSPath(absOriginal);
            Path appendLogPath = ShadowFileSystem.resolveShadowFSAppendLogFilePath(absOriginal);

            // 确保父目录存在
            Path parent = shadowPath.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            // 如果有append log，需要重建
            if (Files.exists(appendLogPath)) {
                ShadowFileChannel channel = new ShadowFileChannel(
                        absOriginal, shadowPath, appendLogPath, null, options);
                return channel;
            }

//            // 如果原始文件存在但shadow不存在
//            if (Files.exists(absOriginal) && !Files.exists(shadowPath)) {
//                // 返回可以写append log的channel
//                FileChannel delegate = FileChannel.open(shadowPath, StandardOpenOption.CREATE,
//                        StandardOpenOption.READ, StandardOpenOption.WRITE);
//                return new ShadowFileChannel(absOriginal, shadowPath, appendLogPath, delegate, options);
//            }
//
//            // 其他情况，直接使用shadow文件
//            if (!Files.exists(shadowPath)) {
//                Files.createFile(shadowPath);
//            }
            return FileChannel.open(shadowPath, options);
        } finally {
            GlobalLockManager.unlock();
        }
    }

    // 辅助类
    private static class AppendingObjectOutputStream extends ObjectOutputStream {
        public AppendingObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // 不写header
        }
    }
}