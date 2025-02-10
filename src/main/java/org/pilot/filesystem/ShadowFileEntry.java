package org.pilot.filesystem;

import java.io.FileOutputStream;
import java.nio.file.Path;

public class ShadowFileEntry {
    private final Path originalPath;
    private final Path shadowPath;

    private Path logPath;

    private boolean contentLoaded;

    private FileOutputStream logStream;


    public ShadowFileEntry(Path originalPath, Path shadowPath) {
        this.originalPath = originalPath;
        this.shadowPath = shadowPath;
        this.contentLoaded = false;
    }

    public Path getOriginalPath() {
        return originalPath;
    }

    public Path getShadowPath() {
        return shadowPath;
    }

    public boolean isContentLoaded() {
        return contentLoaded;
    }

    public void setContentLoaded(boolean contentLoaded) {
        this.contentLoaded = contentLoaded;
    }
}
