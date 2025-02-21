package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public class ShadowFileOutputStream {
    public static FileOutputStream initShadowFileOutputStream(String filePath) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return new FileOutputStream(filePath);
        }

        String shadowFilePath = ShadowFileSystem.getShadowFSPathString(filePath);
        return new FileOutputStream(shadowFilePath);
    }



    public static BufferedWriter initShadowBufferedWriter(Path path, Charset cs,
                                                   OpenOption... options) throws IOException {
        if (!PilotUtil.isDryRun()) {
            return Files.newBufferedWriter(path,cs, options);
        }

        Path shadowFilePath = ShadowFileSystem.resolveShadowFSPath(path);
        return Files.newBufferedWriter(shadowFilePath, cs, options);
    }
}
