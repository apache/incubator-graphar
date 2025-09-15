package org.apache.graphar.info;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class VertexInfoTest {

    private VertexInfo.VertexInfoBuilder b =
            VertexInfo.builder()
                    .baseUri("test")
                    .type("test")
                    .chunkSize(24)
                    .version("gar/v1")
                    .propertyGroups(new PropertyGroups(List.of(TestUtil.pg3)));

    @Test
    public void testVertexInfoBasicBuilder() {
        VertexInfo v = b.build();
    }

    @Test
    public void testVertexInfoBuilderDoubleDeclaration() throws URISyntaxException {
        VertexInfo v = b.baseUri(new URI("world")).build();

        Assert.assertEquals(new URI("test"), v.getBaseUri());
    }
}
