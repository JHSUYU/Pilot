package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.*;
import java.util.Arrays;

import static org.pilot.filesystem.ShadowFileSystem.fileEntries;
import static org.pilot.filesystem.ShadowFileSystem.resolveShadowPath;

public class ShadowFileChannel extends FileChannel {
    private final FileChannel delegate;
    @SuppressWarnings("unused")
    private final ShadowFileEntry entry;

    public ShadowFileChannel(FileChannel delegate, ShadowFileEntry entry) {
        this.delegate = delegate;
        this.entry = entry;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return delegate.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return 0;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return delegate.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return delegate.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return 0;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return delegate.write(src, position);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return null;
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        delegate.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        delegate.truncate(size);
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        delegate.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return delegate.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return delegate.transferFrom(src, position, count);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return delegate.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        delegate.close();
    }

    public static FileChannel open(Path originalPath, OpenOption... options) throws IOException {
        if(!PilotUtil.isDryRun()){
            return FileChannel.open(originalPath, options);
        }
        PilotUtil.dryRunLog("ShadowFileChannel.open"+originalPath.toString());
        ShadowFileSystem.initializeFromOriginal();
        Path absOriginal = originalPath.toAbsolutePath();

        ShadowFileEntry entry = fileEntries.get(absOriginal);
        if (entry == null) {
            Path shadowPath = resolveShadowPath(absOriginal);
            if (!Files.exists(shadowPath)) {
                Files.createFile(shadowPath);
            }
            entry = new ShadowFileEntry(absOriginal, shadowPath);
            fileEntries.put(absOriginal, entry);
        }
        if (!entry.isContentLoaded() && Files.exists(absOriginal) && Files.isRegularFile(absOriginal)) {
            Files.copy(absOriginal, entry.getShadowPath(), StandardCopyOption.REPLACE_EXISTING);
            entry.setContentLoaded(true);
        }

        OpenOption[] newOptions = Arrays.stream(options)
                .filter(opt -> opt != StandardOpenOption.CREATE_NEW)
                .toArray(OpenOption[]::new);

        FileChannel fileChannel = FileChannel.open(entry.getShadowPath(), newOptions);
        return fileChannel;
    }
}
