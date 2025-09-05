package org.apache.graphar.info;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class EdgeInfoTest {

    private EdgeInfo.EdgeInfoBuilder e = EdgeInfo.builder()
            .srcType("person")
            .edgeType("knows")
            .chunkSize(1024)
            .srcChunkSize(100)
            .dstChunkSize(100)
            .directed(false)
            .prefix("edge/person_knows_person/")
            .version("gar/v1");


    @Test
    public void erroneousTripletEdgeBuilderTest(){
        try {
            e.adjacentLists(List.of(TestUtil.orderedBySource, TestUtil.orderedByDest))
                    .addPropertyGroups(List.of(TestUtil.pg3))
                    .build();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void emptyAdjacentListEdgeBuilderTest(){
        try {
            e.dstType("person")
                    .addPropertyGroups(List.of(TestUtil.pg3))
                    .build();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void emptyPropertyGroupsEdgeBuilderTest(){
        try {
            e.adjacentLists(List.of(TestUtil.orderedBySource, TestUtil.orderedByDest))
                    .dstType("person")
                    .build();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
}
