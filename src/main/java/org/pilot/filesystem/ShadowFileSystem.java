package org.pilot.filesystem;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

class ShadowFileSystem {
    // 保存原始文件的绝对路径与对应 ShadowFileEntry 的映射
    private final Map<Path, ShadowFileEntry> fileEntries = new HashMap<>();
    // 记录在 shadow 层已被删除的文件（使用原始文件的绝对路径）
    private final Set<Path> deletedFiles = new HashSet<>();
    // shadow 文件存放的根目录，本示例中设为当前目录下的 "shadow" 文件夹
    private final Path shadowBaseDir;

    public ShadowFileSystem() throws IOException {
        shadowBaseDir = Paths.get("shadow");
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
    public void initializeFromOriginal(Path originalRoot) throws IOException {
        Files.walkFileTree(originalRoot, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path absDir = dir.toAbsolutePath();
                Path shadowDir = resolveShadowPath(absDir);
                if (!Files.exists(shadowDir)) {
                    Files.createDirectories(shadowDir);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path absFile = file.toAbsolutePath();
                Path shadowFile = resolveShadowPath(absFile);
                // 如果 shadow 文件不存在，则创建一个空占位文件
                if (!Files.exists(shadowFile)) {
                    Files.createFile(shadowFile);
                    // 可选：同步元数据，如最后修改时间
                    Files.setLastModifiedTime(shadowFile, Files.getLastModifiedTime(absFile));
                }
                // 记录映射，标记内容尚未加载
                fileEntries.put(absFile, new ShadowFileEntry(absFile, shadowFile));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * 将原始文件或目录的绝对路径映射为 shadow 文件系统中的路径，
     * 保持与原始文件系统相同的目录结构。
     *
     * 例如，原始文件 "/data/sub/file.txt" 对应 shadow 路径为
     * shadowBaseDir.resolve("data/sub/file.txt")
     *
     * @param absOriginal 原始文件或目录的绝对路径
     * @return 对应的 shadow 路径
     * @throws IOException
     */
    private Path resolveShadowPath(Path absOriginal) throws IOException {
        // 这里假定原始文件系统的根目录与 shadow 系统没有公共前缀，
        // 因此直接使用整个相对路径拼接到 shadowBaseDir 下
        Path relativePath = absOriginal.subpath(0, absOriginal.getNameCount());
        Path shadowPath = shadowBaseDir.resolve(relativePath);
        if (!Files.exists(shadowPath.getParent())) {
            Files.createDirectories(shadowPath.getParent());
        }
        return shadowPath;
    }

    /**
     * 打开指定原始文件对应的 shadow 文件（支持 lazy copy / copy‑on‑write）。
     * 如果映射中已有记录但 contentLoaded 为 false，则首次打开时将原始文件内容复制到 shadow 文件中。
     *
     * @param originalPath 原始文件路径（可以是相对路径或绝对路径）
     * @param options      打开选项
     * @return ShadowFileChannel 对象
     * @throws IOException
     */
    public synchronized ShadowFileChannel open(Path originalPath, OpenOption... options) throws IOException {
        Path absOriginal = originalPath.toAbsolutePath();
        if (deletedFiles.contains(absOriginal)) {
            throw new NoSuchFileException("File has been deleted in shadow system: " + absOriginal);
        }
        ShadowFileEntry entry = fileEntries.get(absOriginal);
        if (entry == null) {
            // 如果没有预加载，则采用原有逻辑创建占位
            Path shadowPath = resolveShadowPath(absOriginal);
            if (!Files.exists(shadowPath)) {
                Files.createFile(shadowPath);
            }
            entry = new ShadowFileEntry(absOriginal, shadowPath);
            fileEntries.put(absOriginal, entry);
        }
        // 如果尚未加载实际内容，则进行懒加载：复制原始文件内容到 shadow 文件中
        if (!entry.isContentLoaded() && Files.exists(absOriginal) && Files.isRegularFile(absOriginal)) {
            Files.copy(absOriginal, entry.getShadowPath(), StandardCopyOption.REPLACE_EXISTING);
            entry.setContentLoaded(true);
        }
        FileChannel fileChannel = FileChannel.open(entry.getShadowPath(), options);
        return new ShadowFileChannel(fileChannel, entry);
    }

    /**
     * 在 shadow 文件系统中创建新文件。
     *
     * @param originalPath 原始文件路径（作为映射 key）
     * @throws IOException 如果原始文件已存在或创建失败
     */
    public synchronized void createFile(Path originalPath) throws IOException {
        Path absOriginal = originalPath.toAbsolutePath();
        if (Files.exists(absOriginal)) {
            throw new FileAlreadyExistsException("Original file already exists: " + absOriginal);
        }
        Path shadowPath = resolveShadowPath(absOriginal);
        Files.createDirectories(shadowPath.getParent());
        Files.createFile(shadowPath);
        ShadowFileEntry entry = new ShadowFileEntry(absOriginal, shadowPath);
        entry.setContentLoaded(true); // 新建文件为空，视为已加载
        fileEntries.put(absOriginal, entry);
    }

    /**
     * 在 shadow 文件系统中创建目录，保持与原始文件系统相同的目录结构。
     *
     * @param originalDir 原始目录路径
     * @throws IOException
     */
    public synchronized void createDirectory(Path originalDir) throws IOException {
        Path absDir = originalDir.toAbsolutePath();
        Path shadowDir = resolveShadowPath(absDir);
        Files.createDirectories(shadowDir);
        System.out.println("Directory created in shadow system: " + shadowDir);
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
    public synchronized void delete(Path originalPath) throws IOException {
        Path absOriginal = originalPath.toAbsolutePath();
        ShadowFileEntry entry = fileEntries.get(absOriginal);
        if (entry != null) {
            Files.deleteIfExists(entry.getShadowPath());
            fileEntries.remove(absOriginal);
        } else {
            Path shadowPath = resolveShadowPath(absOriginal);
            Files.deleteIfExists(shadowPath);
        }
        deletedFiles.add(absOriginal);
    }

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
        Path shadowDir = resolveShadowPath(absDirectory);
        List<Path> res = Files.list(shadowDir)
                .map(Path::toAbsolutePath)
                .collect(Collectors.toList());
        return res;
    }
}
