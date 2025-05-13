package org.pilot.runtime;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.UUID;

/**
 * A custom ClassLoader for loading Pilot module JARs on demand.
 * Each instance provides an isolated namespace for the loaded classes.
 */
public class CustomPilotClassLoader extends URLClassLoader {

    private final String id; // Unique ID for tracking this ClassLoader instance

    /**
     * Constructs a new CustomPilotClassLoader.
     *
     * @param urls   The URLs from which to load classes and resources (should point to module JARs).
     * @param parent The parent ClassLoader (typically the application's main ClassLoader).
     */
    public CustomPilotClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.id = "PilotClassLoader-" + UUID.randomUUID().toString().substring(0, 8);
        System.out.println("[ClassLoader] Created new CustomPilotClassLoader: " + id + " (Parent: " + (parent != null ? parent.toString() : "null") + ")");
    }

    /**
     * Returns the unique ID of this ClassLoader instance.
     * @return The ID.
     */
    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }

}