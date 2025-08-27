package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.pilot.PilotUtil.debug;

public class ShadowFileSystem {
    // Keep as ConcurrentHashMap for internal thread-safety, but all access is still protected by global lock
    public static final Map<Path, ShadowFileState> fileEntries = new ConcurrentHashMap<>();
    public static final Set<Path> deletedFiles = new HashSet<>();

    // shadow 文件存放的根目录，本示例中设为当前目录下的 "shadow" 文件夹
    public static Path shadowBaseDir = Paths.get("/opt/ShadowDirectory");

    public static Path shadowAppendLogDir = Paths.get("/opt/ShadowAppendLog");

    // CASSANDRA
    // public static Path originalRoot = Paths.get("/opt/cassandra_data");

    // Solr
    public static Path originalRoot = Paths.get("/opt/SolrData");

//    public static Path shadowBaseDir = Paths.get("/Users/lizhenyu/Desktop/Evaluation/cassandra-correct-version/ShadowDirectory");
//    public static Path shadowAppendLogDir = Paths.get("/Users/lizhenyu/Desktop/Evaluation/cassandra-correct-version/ShadowAppendLog");
//    public static Path originalRoot = Paths.get("/Users/lizhenyu/Desktop/Evaluation/cassandra-correct-version/TempDir");

    private static volatile boolean initialized = false;

    public ShadowFileSystem(Path shadowBaseDir) throws IOException {
        assert shadowBaseDir != null;
        if (!Files.exists(shadowBaseDir)) {
            Files.createDirectories(shadowBaseDir);
        }
    }

    public static void initializeFromOriginal() throws IOException {
        if (debug) {
            return;
        }

        // This method is called within lock context from other classes
        // If called directly, it should acquire the lock
        boolean needsLock = !GlobalLockManager.isHeldByCurrentThread();
        if (needsLock) {
            GlobalLockManager.lock();
        }
        try {
            if (initialized) {
                return;
            }

            if (Files.exists(shadowBaseDir)) {
                initialized = true;
                return;
            }

            Files.createDirectories(shadowBaseDir);
            Files.createDirectories(shadowAppendLogDir);

            Files.walkFileTree(originalRoot, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Path absDir = dir.toAbsolutePath();
                    Path shadowDir = resolveShadowFSPath(absDir);
                    if (!Files.exists(shadowDir)) {
                        Files.createDirectories(shadowDir);
                    }
                    Path shadowLogDir = resolveShadowFSAppendLogDirPath(absDir);
                    if (!Files.exists(shadowLogDir)) {
                        Files.createDirectories(shadowLogDir);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Path absFile = file.toAbsolutePath();
                    Path shadowFile = resolveShadowFSPath(absFile);
                    Path shadowLogFile = resolveShadowFSAppendLogFilePath(absFile);
                    if (!Files.exists(shadowFile)) {
                        Files.createFile(shadowFile);
                    }
                    if (!Files.exists(shadowLogFile)) {
                        Files.createFile(shadowLogFile);
                    }
                    fileEntries.put(absFile, new ShadowFileState(absFile, shadowBaseDir));
                    return FileVisitResult.CONTINUE;
                }
            });

            initialized = true;
        } finally {
            if (needsLock) {
                GlobalLockManager.unlock();
            }
        }
    }

    public static Path resolveShadowFSPath(Path absOriginal) throws IOException {
        // This method is called within lock context, no additional locking needed
        if (absOriginal.toAbsolutePath().startsWith(shadowBaseDir.toAbsolutePath())) {
            PilotUtil.dryRunLog("File is already under the shadow base directory. No need to resolve." + absOriginal);
            return absOriginal;
        }
        PilotUtil.dryRunLog("absOriginal: " + absOriginal);
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        PilotUtil.dryRunLog("relativePath: " + relativePath);
        Path shadowPath = shadowBaseDir.resolve(relativePath);
        PilotUtil.dryRunLog("shadowPath: " + shadowPath);
        return shadowPath;
    }

    public static String getShadowFSPathString(String absOriginalStr) throws IOException {
        // This method is called within lock context, no additional locking needed
        Path absOriginal = Paths.get(absOriginalStr);

        if (absOriginal.toAbsolutePath().startsWith(shadowBaseDir.toAbsolutePath())) {
            PilotUtil.dryRunLog("File is already under the shadow base directory. No need to resolve." + absOriginalStr);
            return absOriginalStr;
        }

        PilotUtil.dryRunLog("absOriginal: " + absOriginalStr);
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        PilotUtil.dryRunLog("relativePath: " + relativePath);

        Path shadowPath = shadowBaseDir.resolve(relativePath);
        PilotUtil.dryRunLog("shadowPath: " + shadowPath);

        return shadowPath.toString();
    }

    public static Path getShadowFSPath(Path absOriginal) {
        // This method is called within lock context, no additional locking needed
        if (absOriginal.toAbsolutePath().startsWith(shadowBaseDir.toAbsolutePath())) {
            System.out.println("File is already under the shadow base directory. No need to resolve." + absOriginal);
            return absOriginal;
        }
        PilotUtil.dryRunLog("absOriginal: " + absOriginal);
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        PilotUtil.dryRunLog("relativePath: " + relativePath);
        Path shadowPath = shadowBaseDir.resolve(relativePath);
        PilotUtil.dryRunLog("shadowPath: " + shadowPath);
        return shadowPath;
    }

    public static Path getOriginalFSPath(Path shadowPath) {
        // This method is called within lock context, no additional locking needed
        Path absShadowPath = shadowPath.toAbsolutePath();
        Path absShadowBaseDir = shadowBaseDir.toAbsolutePath();

        // 检查路径是否在 shadow 目录下
        if (!absShadowPath.startsWith(absShadowBaseDir)) {
            PilotUtil.dryRunLog("Path is not under shadow base directory, assuming it's already original: " + shadowPath);
            // 如果不在 shadow 目录下，可能已经是原始路径
            return shadowPath;
        }

        // 获取相对于 shadowBaseDir 的路径
        Path relativePath = absShadowBaseDir.relativize(absShadowPath);
        PilotUtil.dryRunLog("relativePath from shadow: " + relativePath);

        // 重建原始路径：使用根路径 "/" 加上相对路径
        Path originalPath = Paths.get("/").resolve(relativePath);
        PilotUtil.dryRunLog("originalPath: " + originalPath);

        return originalPath;
    }

    public static String getOriginalFSPathString(String shadowPathStr) {
        Path shadowPath = Paths.get(shadowPathStr);
        Path absShadowPath = shadowPath.toAbsolutePath();
        Path absShadowBaseDir = shadowBaseDir.toAbsolutePath();

        // 检查路径是否在 shadow 目录下
        if (!absShadowPath.startsWith(absShadowBaseDir)) {
            PilotUtil.dryRunLog("Path is not under shadow base directory, assuming it's already original: " + shadowPathStr);
            return shadowPathStr;
        }

        // 获取相对于 shadowBaseDir 的路径
        Path relativePath = absShadowBaseDir.relativize(absShadowPath);
        PilotUtil.dryRunLog("relativePath from shadow: " + relativePath);

        // 重建原始路径
        Path originalPath = Paths.get("/").resolve(relativePath);
        PilotUtil.dryRunLog("originalPath: " + originalPath);

        return originalPath.toString();
    }

    public static Path resolveShadowFSAppendLogFilePath(Path absOriginal) {
        // This method is called within lock context, no additional locking needed
        if (absOriginal.toAbsolutePath().startsWith(shadowBaseDir.toAbsolutePath())) {
            PilotUtil.dryRunLog("File is already under the shadow base directory. No need to resolve." + absOriginal);
            return absOriginal;
        }
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        Path shadowAppendLogPath = shadowAppendLogDir.resolve(relativePath).resolveSibling(
                absOriginal.getFileName().toString() + ".log"
        );
        return shadowAppendLogPath;
    }

    public static Path resolveShadowFSAppendLogDirPath(Path absOriginal) throws IOException {
        // This method is called within lock context, no additional locking needed
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        Path shadowPath = shadowAppendLogDir.resolve(relativePath);
        if (!Files.exists(shadowPath.getParent())) {
            Files.createDirectories(shadowPath.getParent());
        }
        return shadowPath;
    }

    public List<Path> listDirectory(Path directory) throws IOException {
        GlobalLockManager.lock();
        try {
            Path absDirectory = directory.toAbsolutePath();
            Path shadowDir = resolveShadowFSPath(absDirectory);
            List<Path> res = Files.list(shadowDir)
                    .map(Path::toAbsolutePath)
                    .collect(Collectors.toList());
            return res;
        } finally {
            GlobalLockManager.unlock();
        }
    }
}