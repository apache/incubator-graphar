package org.apache.graphar.info;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.graphar.info.loader.StreamGraphInfoLoader;

public class LocalFileSystemStringStreamLoader extends StreamGraphInfoLoader {
    @Override
    public InputStream readYaml(String path) throws IOException {
        return new FileInputStream(path);
    }
}
