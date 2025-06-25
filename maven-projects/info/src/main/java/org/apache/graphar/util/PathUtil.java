package org.apache.graphar.util;

public class PathUtil {
    public static String pathToDirectory(String path) {
        if (path.startsWith("s3://")) {
            int t = path.indexOf('?');
            if (t != -1) {
                String prefix = path.substring(0, t);
                String suffix = path.substring(t);
                int lastSlashIdx = prefix.lastIndexOf('/');
                if (lastSlashIdx != -1) {
                    return prefix.substring(0, lastSlashIdx + 1) + suffix;
                }
            } else {
                int lastSlashIdx = path.lastIndexOf('/');
                if (lastSlashIdx != -1) {
                    return path.substring(0, lastSlashIdx + 1);
                }
                return path;
            }
        } else {
            int lastSlashIdx = path.lastIndexOf('/');
            if (lastSlashIdx != -1) {
                return path.substring(0, lastSlashIdx + 1);
            }
        }
        return path;
    }
}
