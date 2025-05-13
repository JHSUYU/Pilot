package org.pilot.runtime;

import java.net.URL;

public class ModuleInfo {
    public String moduleName;
    public URL moduleJarUrl;
    public String pilotEntryClassName;

    public ModuleInfo(String moduleName, URL moduleJarUrl, String pilotEntryClassName) {
        this.moduleName = moduleName;
        this.moduleJarUrl = moduleJarUrl;
        this.pilotEntryClassName = pilotEntryClassName;
    }
}
