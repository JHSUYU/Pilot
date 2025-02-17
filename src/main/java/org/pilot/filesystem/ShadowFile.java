package org.pilot.filesystem;

import javafx.scene.effect.Shadow;
import org.pilot.PilotUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.pilot.filesystem.ShadowFileSystem.shadowBaseDir;

public class ShadowFile{
    private String fileName;
    private String filePath;
    private long fileSize;
    private long lastModifiedTime;


    public static File initFile(File parent, String child){
        if(!PilotUtil.isDryRun()){
            return new File(parent, child);
        }

        try{
            ShadowFileSystem.initializeFromOriginal();
            Path originalFilePath = Paths.get(parent.getAbsolutePath(), child);
            System.out.println("Original file path: " + originalFilePath.toString());

            Path shadowFilePath = ShadowFileSystem.resolveShadowFSPath(originalFilePath);
            System.out.println("Shadow file path: " + shadowFilePath.toString());
            return shadowFilePath.toFile();
        }catch(IOException e){
            System.out.println("Error initializing ShadowFileSystem: " + e.getMessage());
        }
        return null;
    }

    public ShadowFile(File parent, String child) {

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
