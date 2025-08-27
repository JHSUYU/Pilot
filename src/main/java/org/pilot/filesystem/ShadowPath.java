package org.pilot.filesystem;

import org.pilot.PilotUtil;

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.Path;

public class ShadowPath {

    public Path delegetePath;
    public ShadowPath(Path delegetePath) {
        this.delegetePath = delegetePath;
    }

    public Path toRealPath(LinkOption... options) throws IOException{
        if(!PilotUtil.isDryRun()){
            return delegetePath.toRealPath(options);
        }

        ShadowFileSystem.initializeFromOriginal();
        Path shadowPath = ShadowFileSystem.getShadowFSPath(delegetePath);
        return shadowPath.toRealPath(options);
    }
}
