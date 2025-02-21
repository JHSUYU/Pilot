package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ShadowFiles {
    public static void delete(Path path) throws IOException {
        if(!PilotUtil.isDryRun()){
            Files.delete(path);
            return;
        }

        Path shadowPath = ShadowFileSystem.getShadowFSPath(path);
        Files.delete(shadowPath);
        return;
    }
}
