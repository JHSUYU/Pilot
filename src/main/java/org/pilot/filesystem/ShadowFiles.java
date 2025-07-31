package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.pilot.PilotUtil.debug;

public class ShadowFiles {
    public static void delete(Path path) throws IOException {
        if(debug || !PilotUtil.isDryRun()){
            Files.delete(path);
            return;
        }

        Path shadowPath = ShadowFileSystem.getShadowFSPath(path);
        Files.delete(shadowPath);
        return;
    }

    public static boolean delete(File file) throws IOException{
        return true;
    }

    public static boolean rename(File src, File dest) throws IOException {
        return true;
    }
}
