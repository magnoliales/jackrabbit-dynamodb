package com.kartashov.jackrabbit.dynamodb;

import org.junit.Test;

import javax.jcr.Node;

public class SimpleRepositoryTest extends AbstractRepositoryTest {

    @Test
    public void directlyWriteNodesAndProperties() throws Exception {
        Node root = session.getRootNode();
        Node australia = session.getRootNode().addNode("australia");
        Node canberra = australia.addNode("canberra");
        canberra.setProperty("capital", true);
        Node sydney = australia.addNode("sydney");
        String sydneyId = sydney.getIdentifier();
        sydney.remove();
        Node melbourne = australia.addNode("melbourne");
        Node perth = australia.addNode("perth");
        Node darwin = australia.addNode("darwin");
        Node brisbane = australia.addNode("brisbane");
        Node adelaide = australia.addNode("adelaide");
        session.save();
    }
}
