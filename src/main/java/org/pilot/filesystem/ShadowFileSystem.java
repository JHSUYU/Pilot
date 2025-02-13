package org.pilot.filesystem;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ShadowFileSystem {
    // 保存原始文件的绝对路径与对应 ShadowFileEntry 的映射
    public static final Map<Path, ShadowFileState> fileEntries = new HashMap<>();
    // 记录在 shadow 层已被删除的文件（使用原始文件的绝对路径）
    public static final Set<Path> deletedFiles = new HashSet<>();
    // shadow 文件存放的根目录，本示例中设为当前目录下的 "shadow" 文件夹
    public static Path shadowBaseDir = Paths.get("/Users/lizhenyu/Desktop/Evaluation/cassandra-13938/ShadowDirectory");

    public static Path shadowAppendLogDir = Paths.get("/Users/lizhenyu/Desktop/Evaluation/cassandra-13938/ShadowAppendLog");

    public static Path originalRoot = Paths.get("/Users/lizhenyu/Desktop/Evaluation/cassandra-13938/TempDir");

    public ShadowFileSystem(Path shadowBaseDir) throws IOException {
        assert shadowBaseDir != null;
        if (!Files.exists(shadowBaseDir)) {
            Files.createDirectories(shadowBaseDir);
        }
    }

    /**
     * 预加载原始文件系统中所有目录和文件元数据（占位信息）。
     * 对于每个目录，在 shadow 层中创建对应目录；
     * 对于每个文件，在 shadow 层中创建一个空文件（占位），并记录映射信息，
     * 同时将 contentLoaded 标记为 false，后续首次访问时进行懒加载内容拷贝。
     *
     * @param originalRoot 原始文件系统根目录
     * @throws IOException
     */
    public static void initializeFromOriginal() throws IOException {
        if(Files.exists(shadowBaseDir)){
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
                if(!Files.exists(shadowFile)){
                    Files.createFile(shadowFile);
                }
                fileEntries.put(absFile, new ShadowFileState(absFile, shadowBaseDir));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static Path resolveShadowFSPath(Path absOriginal) throws IOException {
        // 这里假定原始文件系统的根目录与 shadow 系统没有公共前缀，
        // 因此直接使用整个相对路径拼接到 shadowBaseDir 下
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        Path shadowPath = shadowBaseDir.resolve(relativePath);
        if (!Files.exists(shadowPath.getParent())) {
            Files.createDirectories(shadowPath.getParent());
        }
        return shadowPath;
    }

    public static Path resolveShadowFSAppendLogFilePath(Path absOriginal) throws IOException {
        // 这里假定原始文件系统的根目录与 shadow 系统没有公共前缀，
        // 因此直接使用整个相对路径拼接到 shadowBaseDir 下
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        Path shadowAppendLogPath = shadowAppendLogDir.resolve(relativePath).resolveSibling(
                absOriginal.getFileName().toString() + ".log"
        );;

        return shadowAppendLogPath;
    }

    public static Path resolveShadowFSAppendLogDirPath(Path absOriginal) throws IOException {
        // 这里假定原始文件系统的根目录与 shadow 系统没有公共前缀，
        // 因此直接使用整个相对路径拼接到 shadowBaseDir 下
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        Path shadowPath = shadowAppendLogDir.resolve(relativePath);
        if (!Files.exists(shadowPath.getParent())) {
            Files.createDirectories(shadowPath.getParent());
        }
        return shadowPath;
    }

    /**
     * 删除文件，将删除操作代理到 shadow 文件系统中：
     * 1. 删除 shadow 文件；
     * 2. 移除映射；
     * 3. 记录删除信息，保证目录列表中不再返回该文件。
     *
     * @param originalPath 原始文件路径
     * @throws IOException
     */
//    public synchronized void delete(Path originalPath) throws IOException {
//        Path absOriginal = originalPath.toAbsolutePath();
//        ShadowFileEntry entry = fileEntries.get(absOriginal);
//        if (entry != null) {
//            Files.deleteIfExists(entry.getShadowPath());
//            fileEntries.remove(absOriginal);
//        } else {
//            Path shadowPath = resolveShadowFSPath(absOriginal);
//            Files.deleteIfExists(shadowPath);
//        }
//        deletedFiles.add(absOriginal);
//    }

    /**
     * 列出指定目录下的文件或子目录，返回 shadow 文件系统中的路径列表，
     * 并过滤掉已在 shadow 层删除的项目。
     *
     * @param directory 指定目录（可以是相对路径或绝对路径）
     * @return 目录下所有 shadow 项目的列表
     * @throws IOException
     */
    public synchronized List<Path> listDirectory(Path directory) throws IOException {
        Path absDirectory = directory.toAbsolutePath();
        Path shadowDir = resolveShadowFSPath(absDirectory);
        List<Path> res = Files.list(shadowDir)
                .map(Path::toAbsolutePath)
                .collect(Collectors.toList());
        return res;
    }
}
