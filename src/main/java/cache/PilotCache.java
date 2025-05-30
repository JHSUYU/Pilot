package cache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Iterator;

public final class PilotCache {

    private static final class CacheKey {
        private final String filePath;
        private final long position;
        private final int length;

        public CacheKey(String filePath, long position, int length) {
            this.filePath = Objects.requireNonNull(filePath, "filePath must not be null");
            this.position = position;
            this.length = length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return position == cacheKey.position &&
                    length == cacheKey.length &&
                    filePath.equals(cacheKey.filePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filePath, position, length);
        }

        @Override
        public String toString() {
            return "CacheKey{" +
                    "filePath='" + filePath + '\'' +
                    ", position=" + position +
                    ", length=" + length +
                    '}';
        }
    }

    private static final class CacheValue {
        private final byte[] data;
        private final long fileLastModifiedTimestamp;

        public CacheValue(byte[] data, long fileLastModifiedTimestamp) {
            this.data = Arrays.copyOf(data, data.length);
            this.fileLastModifiedTimestamp = fileLastModifiedTimestamp;
        }

        public byte[] getData() {
            return Arrays.copyOf(this.data, this.data.length);
        }

        public long getFileLastModifiedTimestamp() {
            return fileLastModifiedTimestamp;
        }

        public int getDataLength() {
            return this.data.length;
        }
    }

    private static final ConcurrentHashMap<CacheKey, CacheValue> cache = new ConcurrentHashMap<>();

    private static int MAX_ENTRIES = 1000;
    private static int DEFAULT_MAX_ENTRIES = 1000; // 默认最大条目数

    // 递增ID计数器
    private static final AtomicLong putIdCounter = new AtomicLong(0);
    private static final AtomicLong getIdCounter = new AtomicLong(0);

    // 日志文件路径
    private static final String PUT_LOG_FILE = "/opt/cache_put.txt";
    private static final String GET_LOG_FILE = "/opt/cache_get.txt";

    /**
     * 工具方法：将操作信息写入指定文件
     * @param filePath 目标日志文件路径
     * @param cacheKey 缓存键
     * @param operationId 操作ID
     */
    private static void writeOperationLog(String filePath, CacheKey cacheKey, long operationId) {
        try {
            String logEntry = String.format("ID: %d, %s%n", operationId, cacheKey.toString());
            Path logPath = Paths.get(filePath);

            // 确保父目录存在
            Path parentDir = logPath.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }

            // 追加写入日志文件
            Files.write(logPath, logEntry.getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("写入操作日志失败 '" + filePath + "': " + e.getMessage());
        }
    }

    private static long getCurrentFileLastModified(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            throw new IOException("文件不存在: " + filePath);
        }
        return Files.getLastModifiedTime(path).toMillis();
    }

    public static byte[] get(String filePath, long position, int length) {
        if (length <= 0) return null;

        CacheKey key = new CacheKey(filePath, position, length);
        CacheValue value = cache.get(key);

        if (value != null) {
            try {
                long currentTimestamp = getCurrentFileLastModified(filePath);
                if (value.getFileLastModifiedTimestamp() == currentTimestamp && value.getDataLength() == length) {
                    byte[] result = value.getData();

                    // 记录成功的get操作
                    long getId = getIdCounter.incrementAndGet();
                    //writeOperationLog(GET_LOG_FILE, key, getId);

                    return result;
                } else {
                    cache.remove(key);
                    return null;
                }
            } catch (IOException e) {
                System.err.println("警告: 检查文件时间戳时出错 '" + filePath + "': " + e.getMessage() + "。缓存条目已失效。");
                cache.remove(key);
                return null;
            }
        }
        return null;
    }

    public static void put(String filePath, long position, byte[] data, long fileLastModifiedTimestamp) {
        if (data == null || data.length == 0) {
            return;
        }

        CacheKey key = new CacheKey(filePath, position, data.length);
        CacheValue value = new CacheValue(data, fileLastModifiedTimestamp);
        cache.put(key, value);

        // 记录put操作
        long putId = putIdCounter.incrementAndGet();
        //writeOperationLog(PUT_LOG_FILE, key, putId);
    }

    public static void clear() {
        cache.clear();
    }

    public static long size() {
        return cache.size();
    }
}