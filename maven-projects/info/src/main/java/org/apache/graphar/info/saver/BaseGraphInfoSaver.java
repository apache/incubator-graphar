package org.apache.graphar.info.saver;

import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.VertexInfo;

public abstract class BaseGraphInfoSaver implements GraphInfoSaver {

    public abstract Writer writeYaml(URI uri) throws IOException;

    @Override
    public void save(URI graphInfoUri, GraphInfo graphInfo) throws IOException {
        Writer writer = writeYaml(graphInfoUri);
        writer.write(graphInfo.dump(graphInfoUri));
        writer.close();

        for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
            URI saveUri = graphInfoUri.resolve(graphInfo.getStoreUri(vertexInfo));
            save(saveUri, vertexInfo);
        }
        for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
            URI saveUri = graphInfoUri.resolve(graphInfo.getStoreUri(edgeInfo));
            save(saveUri, edgeInfo);
        }
    }

    @Override
    public void save(URI vertexInfoUri, VertexInfo vertexInfo) throws IOException {
        Writer vertexWriter = writeYaml(vertexInfoUri);
        vertexWriter.write(vertexInfo.dump());
        vertexWriter.close();
    }

    @Override
    public void save(URI edgeInfoUri, EdgeInfo edgeInfo) throws IOException {
        Writer edgeWriter = writeYaml(edgeInfoUri);
        edgeWriter.write(edgeInfo.dump());
        edgeWriter.close();
    }
}
