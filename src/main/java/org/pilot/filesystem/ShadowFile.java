package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.pilot.PilotUtil.debug;
import static org.pilot.filesystem.ShadowFileSystem.shadowBaseDir;

public class ShadowFile {
    private String fileName;
    private String filePath;
    private long fileSize;
    private long lastModifiedTime;

    public static File initFile(File parent, String child) {
        GlobalLockManager.lock();
        try {
            if (debug || !PilotUtil.isDryRun()) {
                return new File(parent, child);
            }

            try {
                ShadowFileSystem.initializeFromOriginal();
                Path originalFilePath = Paths.get(parent.getAbsolutePath(), child);

                PilotUtil.dryRunLog("Original file path: " + originalFilePath.toString());

                Path shadowFilePath = ShadowFileSystem.resolveShadowFSPath(originalFilePath);

                PilotUtil.dryRunLog("Shadow file path: " + shadowFilePath.toString());
                return shadowFilePath.toFile();
            } catch (IOException e) {
                PilotUtil.dryRunLog("Error1 initializing ShadowFileSystem: " + e.getMessage() + e);
            }
            return null;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    public static File initFile(String pathname) {
        GlobalLockManager.lock();
        try {
            if (debug || !PilotUtil.isDryRun()) {
                return new File(pathname);
            }

            try {
                String shadowFilePath = ShadowFileSystem.getShadowFSPathString(pathname);

                File shadowFile = new File(shadowFilePath);
                if (!shadowFile.exists() && !Files.exists(shadowBaseDir)) {
                    PilotUtil.dryRunLog("Shadow file does not exist, initializing ShadowFileSystem.");
                    ShadowFileSystem.initializeFromOriginal();
                }

                PilotUtil.dryRunLog("Shadow file path: " + shadowFilePath);
                return shadowFile;
            } catch (IOException e) {
                PilotUtil.dryRunLog("Error2 initializing ShadowFileSystem: " + e.getMessage() + e);
            }
            return null;
        } finally {
            GlobalLockManager.unlock();
        }
    }

    public ShadowFile(File parent, String child) {
        // Constructor implementation
    }

    public String getFileName() {
        return fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }
}