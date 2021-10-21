package org.neo4japps.webgraph.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public final class DirectoryUtil {

    private DirectoryUtil() {

    }

    public static void deleteDir(String dirName) throws IOException {
        deleteDir(new File(dirName));
    }

    public static void deleteDir(File dir) throws IOException {
        if (!dir.exists())
            return;

        if (dir.isDirectory()) {
            String[] children = dir.list();
            if (children == null) {
                return;
            }
            for (String child : children) {
                deleteDir(new File(dir, child));
            }
        }

        // The directory is now empty so delete it
        // NB The Files class was introduced in Java 7, this won't compile in earlier Java versions
        Files.delete(dir.toPath());
    }
}
