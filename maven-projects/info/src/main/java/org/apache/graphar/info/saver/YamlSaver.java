package org.apache.graphar.info.saver;

import java.io.IOException;

@FunctionalInterface
public interface YamlSaver {
    void saveYaml(String path, String yaml) throws IOException;
}
