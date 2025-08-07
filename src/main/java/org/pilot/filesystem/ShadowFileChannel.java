package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.*;
import java.util.Arrays;

import static org.pilot.PilotUtil.debug;
import static org.pilot.filesystem.ShadowFileSystem.fileEntries;

public class ShadowFileChannel extends FileChannel {
    private FileChannel delegate;
    private FileChannel originalChannel = null;
    private ShadowFileState shadowFileState;
    public OpenOption[] options;
    private long currentPosition = 0;

    public ShadowFileState getShadowFileState() {
        return shadowFileState;
    }

    private long rebuildFromLog() throws IOException {
        // This is already called within a lock context
        long start = System.currentTimeMillis();

        Path tempRebuiltPath = shadowFileState.reconstructedPath;
        Files.deleteIfExists(tempRebuiltPath);
        Files.createFile(tempRebuiltPath);
        long position = 0;

        try (FileChannel rebuiltChannel = FileChannel.open(tempRebuiltPath,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // 1. Copy original file content
            try (FileChannel originalChannel = FileChannel.open(
                    shadowFileState.getOriginalPath(), StandardOpenOption.READ)) {
                originalChannel.transferTo(0, originalChannel.size(), rebuiltChannel);
            }

            // 2. Apply all write operations
            for (FileOperation operation : shadowFileState.getOperations()) {
                ByteBuffer buffer = ByteBuffer.wrap(operation.getData());
                rebuiltChannel.position(operation.getOffset());
                PilotUtil.dryRunLog("operation.getOffset()" + operation.getOffset());
                rebuiltChannel.write(buffer);
                position = rebuiltChannel.position();
            }
        }
        PilotUtil.recordTime(System.currentTimeMillis() - start, "/users/ZhenyuLi/rebuildFromLog.txt");
        return position;
    }

    public ShadowFileChannel(FileChannel delegate, ShadowFileState shadowFileState) {
        this.delegate = delegate;
        this.shadowFileState = shadowFileState;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.read(dst);
            }
            if (Files.exists(shadowFileState.getAppendLogPath())) {
                long position = rebuildFromLog();
                this.delegate = FileChannel.open(shadowFileState.reconstructedPath, options);
                this.delegate.position(position);
                shadowFileState.existBeforePilot = false;
                Files.deleteIfExists(shadowFileState.getAppendLogPath());
                return delegate.read(dst);
            } else {
                if (originalChannel == null) {
                    originalChannel = FileChannel.open(shadowFileState.getOriginalPath(), options);
                }
                return originalChannel.read(dst);
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.read(dsts, offset, length);
            }
            if (Files.exists(shadowFileState.getAppendLogPath())) {
                long position = rebuildFromLog();
                this.delegate = FileChannel.open(shadowFileState.reconstructedPath, options);
                shadowFileState.existBeforePilot = false;
                Files.deleteIfExists(shadowFileState.getAppendLogPath());
                this.delegate.position(position);
                return delegate.read(dsts, offset, length);
            } else {
                if (originalChannel == null) {
                    originalChannel = FileChannel.open(shadowFileState.getOriginalPath(), options);
                }
                return originalChannel.read(dsts, offset, length);
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    private long copyOriginalFile() throws IOException {
        // This is already called within a lock context
        Path tempRebuiltPath = shadowFileState.reconstructedPath;
        Files.deleteIfExists(tempRebuiltPath);
        Files.createFile(tempRebuiltPath);

        try (FileChannel rebuiltChannel = FileChannel.open(tempRebuiltPath,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileChannel originalChannel = FileChannel.open(
                     shadowFileState.getOriginalPath(), StandardOpenOption.READ)) {

            originalChannel.transferTo(0, originalChannel.size(), rebuiltChannel);
            return rebuiltChannel.size();
        }
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.read(dst, position);
            }
            if (Files.exists(shadowFileState.getAppendLogPath())) {
                long tmpPosition = rebuildFromLog();
                this.delegate = FileChannel.open(shadowFileState.reconstructedPath, options);
                shadowFileState.existBeforePilot = false;
                Files.deleteIfExists(shadowFileState.getAppendLogPath());
                this.delegate.position(tmpPosition);
                return delegate.read(dst, position);
            } else {
                if (originalChannel == null) {
                    originalChannel = FileChannel.open(shadowFileState.getOriginalPath(), options);
                }
                return originalChannel.read(dst);
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.write(src);
            }

            byte[] data = new byte[src.remaining()];
            src.get(data);

            setCurrentPositionFromOriginalChannel();
            FileOperation operation = new FileOperation(currentPosition, data, "WRITE");
            shadowFileState.recordOperation(operation);
            currentPosition += data.length;
            return data.length;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.write(srcs, offset, length);
            }

            // Calculate total length
            long totalLength = 0;
            for (int i = offset; i < offset + length; i++) {
                totalLength += srcs[i].remaining();
            }

            // Create a large data array
            byte[] combinedData = new byte[(int) totalLength];
            int position = 0;

            // Copy all data
            for (int i = offset; i < offset + length; i++) {
                ByteBuffer src = srcs[i];
                int len = src.remaining();
                src.get(combinedData, position, len);
                position += len;
            }

            // Create single FileOperation
            setCurrentPositionFromOriginalChannel();
            FileOperation operation = new FileOperation(currentPosition, combinedData, "WRITE");
            shadowFileState.recordOperation(operation);
            currentPosition += totalLength;
            return totalLength;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.write(src, position);
            }

            byte[] data = new byte[src.remaining()];
            src.get(data);

            setCurrentPositionFromOriginalChannel();
            FileOperation operation = new FileOperation(position, data, "WRITE");
            shadowFileState.recordOperation(operation);
            return data.length;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return null;
    }

    @Override
    public long position() throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.position();
            }
            if (Files.exists(shadowFileState.getAppendLogPath())) {
                long position = rebuildFromLog();
                this.delegate = FileChannel.open(shadowFileState.reconstructedPath, options);
                shadowFileState.existBeforePilot = false;
                Files.deleteIfExists(shadowFileState.getAppendLogPath());
                this.delegate.position(position);
                return position;
            } else {
                if (originalChannel == null) {
                    originalChannel = FileChannel.open(shadowFileState.getOriginalPath(), options);
                }
                return originalChannel.position();
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                delegate.position(newPosition);
                return this;
            }
            if (Files.exists(shadowFileState.getAppendLogPath())) {
                rebuildFromLog();
                this.delegate = FileChannel.open(shadowFileState.reconstructedPath, options);
                shadowFileState.existBeforePilot = false;
                Files.deleteIfExists(shadowFileState.getAppendLogPath());
                this.delegate.position(newPosition);
                return this;
            } else {
                if (originalChannel == null) {
                    originalChannel = FileChannel.open(shadowFileState.getOriginalPath(), options);
                }
                originalChannel.position(newPosition);
                return this;
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long size() throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.size();
            }
            if (Files.exists(shadowFileState.getAppendLogPath())) {
                rebuildFromLog();
                this.delegate = FileChannel.open(shadowFileState.reconstructedPath, options);
                shadowFileState.existBeforePilot = false;
                Files.deleteIfExists(shadowFileState.getAppendLogPath());
                return delegate.size();
            } else {
                if (originalChannel == null) {
                    originalChannel = FileChannel.open(shadowFileState.getOriginalPath(), options);
                }
                return originalChannel.size();
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                delegate.truncate(size);
                return this;
            }

            if (Files.exists(shadowFileState.getAppendLogPath())) {
                rebuildFromLog();
                this.delegate = FileChannel.open(shadowFileState.reconstructedPath, options);
                shadowFileState.existBeforePilot = false;
                Files.deleteIfExists(shadowFileState.getAppendLogPath());
                this.delegate.truncate(size);
                return this.delegate;
            } else {
                // Copy shadowFileState.getOriginalPath() file to shadowFileState.reconstructedPath
                copyOriginalFile();
                this.delegate = FileChannel.open(shadowFileState.reconstructedPath, options);
                shadowFileState.existBeforePilot = false;
                this.delegate.truncate(size);
                return this.delegate;
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                delegate.force(metaData);
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.transferTo(position, count, target);
            }
            return 0;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.transferFrom(src, position, count);
            }
            return 0;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.lock(position, size, shared);
            }
            return null;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                return delegate.tryLock(position, size, shared);
            }
            return null;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    @Override
    protected void implCloseChannel() throws IOException {
        GlobalLockManager.lock();
        try {
            if (!shadowFileState.existBeforePilot) {
                delegate.close();
                return;
            }
        } finally {
            GlobalLockManager.unlock();
        }
    }

    public void setCurrentPositionFromOriginalChannel() {
        // This is called within lock context, no need for additional locking
        if (originalChannel != null) {
            try {
                currentPosition = originalChannel.position();
            } catch (IOException e) {
                e.printStackTrace();
                PilotUtil.dryRunLog("ShadowFileChannel.setCurrentPositionFromOriginalChannel" + e.getMessage());
            }
        }
    }

    public static FileChannel open(Path originalPath, OpenOption... options) throws IOException {
        GlobalLockManager.lock();
        try {
            if (debug || !PilotUtil.isDryRun()) {
                return FileChannel.open(originalPath, options);
            }
            PilotUtil.dryRunLog("ShadowFileChannel.open" + originalPath.toString());
            try {
                ShadowFileSystem.initializeFromOriginal();
            } catch (IOException e) {
                PilotUtil.dryRunLog("Error3 initializing ShadowFileSystem: " + e.getMessage() + e);
                throw new IOException("Failed to initialize ShadowFileSystem", e);
            }
            Path absOriginal = originalPath.toAbsolutePath();

            Path shadowFile = ShadowFileSystem.resolveShadowFSPath(absOriginal);
            boolean exist = Files.exists(shadowFile);
            if (!exist) {
                PilotUtil.dryRunLog("create file" + shadowFile.toString());
                try {
                    Files.createFile(shadowFile);
                } catch (IOException e) {
                    PilotUtil.dryRunLog("File already created by another thread: " + shadowFile);
                    throw e;
                }
            }

            OpenOption[] newOptions = Arrays.stream(options)
                    .filter(opt -> opt != StandardOpenOption.CREATE_NEW)
                    .toArray(OpenOption[]::new);

            FileChannel fileChannel = FileChannel.open(shadowFile, newOptions);

            ShadowFileState shadowFileState = fileEntries.get(absOriginal);
            if (shadowFileState == null) {
                shadowFileState = new ShadowFileState(absOriginal, ShadowFileSystem.shadowBaseDir);
                shadowFileState.existBeforePilot = false;
                shadowFileState.reconstructedPath = shadowFile;
                shadowFileState.setAppendLogPath(ShadowFileSystem.resolveShadowFSAppendLogFilePath(absOriginal));
                fileEntries.put(absOriginal, shadowFileState);
            } else {
                if (shadowFileState.existBeforePilot) {
                    shadowFileState.existBeforePilot = exist;
                }
                shadowFileState.reconstructedPath = shadowFile;
                shadowFileState.setAppendLogPath(ShadowFileSystem.resolveShadowFSAppendLogFilePath(absOriginal));
                fileEntries.put(absOriginal, shadowFileState);
            }
            Files.deleteIfExists(shadowFileState.getAppendLogPath());
            ShadowFileChannel res = new ShadowFileChannel(fileChannel, shadowFileState);
            if (!shadowFileState.existBeforePilot) {
                PilotUtil.dryRunLog("return normal fileChannel with path" + shadowFile.toAbsolutePath());
                return fileChannel;
            }
            res.options = newOptions;
            return res;
        } finally {
            GlobalLockManager.unlock();
        }
    }
}