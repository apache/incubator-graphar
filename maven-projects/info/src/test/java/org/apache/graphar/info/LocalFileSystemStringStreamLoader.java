package org.apache.graphar.info;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.graphar.info.loader.StreamGraphInfoLoader;

public class LocalFileSystemStringStreamLoader extends StreamGraphInfoLoader {
    @Override
    public InputStream readYaml(URI uri) throws IOException {
        if (!"file".equals(uri.getScheme())) {
            throw new RuntimeException("Only file:// scheme is supported in Local File System");
        }
        String path = uri.getPath();
        return new FileInputStream(path);
    }
}
