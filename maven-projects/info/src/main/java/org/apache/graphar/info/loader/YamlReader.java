package org.apache.graphar.info.loader;

import java.io.IOException;

@FunctionalInterface
public interface YamlReader {
    String readYaml(String path) throws IOException;
}
