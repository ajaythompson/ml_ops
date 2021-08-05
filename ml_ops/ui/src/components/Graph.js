import {
    GraphView, // required
    Edge, // optional
    type IEdge, // optional
    Node, // optional
    type INode, // optional
    type LayoutEngineType, // required to change the layoutEngineType, otherwise optional
    BwdlTransformer, // optional, Example JSON transformer
    GraphUtils // optional, useful utility functions
} from 'react-digraph';

import React from 'react';
import { addRelation, updateProcessor } from '../api/Workbench'

const GraphConfig = {
    NodeTypes: {
        node: { // required to show empty nodes
            // typeText: "Processor",
            shapeId: "#node", // relates to the type property of a node
            shape: (
                <symbol viewBox="0 0 90 30" id="node" key="0">
                    <rect cx="50" cy="50" height="50" width="140" ></rect>
                </symbol>
            )
        },
        custom: { // required to show empty nodes
            typeText: "Custom",
            shapeId: "#custom", // relates to the type property of a node
            shape: (
                <symbol viewBox="0 0 50 25" id="custom" key="0">
                    <ellipse cx="50" cy="25" rx="50" ry="25"></ellipse>
                </symbol>
            )
        }
    },
    NodeSubtypes: {},
    EdgeTypes: {
        edge: {  // required to show empty edges
            shapeId: "#edge",
            shape: (
                <symbol viewBox="0 0 50 50" id="edge" key="0">
                    <circle cx="25" cy="25" r="8" fill="currentColor"> </circle>
                </symbol>
            )
        }
    }
}

const NODE_KEY = "id"       // Allows D3 to correctly update DOM

class Graph extends React.Component {


    constructor(props) {
        super(props);
    }

    /* Define custom graph editing methods here */
    onCreateNode() {
        this.setState({})
        // console.log(this.state)
    }

    onSelect(event) {
        console.log('selecting')
        console.log(event)
    }

    onBackgroundClick() {
        console.log(this)
    }

    onCreateEdge(source, target) {
        const workflowId = this.props.graph.id

        const data = {
            "source": source.id,
            "target": target.id
        }

        addRelation(workflowId, data)
            .then(
                resp => {
                    this.props.setWorkflow(resp.data)
                }
            )

    }

    onUpdateNode(node) {
        console.log('updating')
        const workflowId = this.props.graph.id
        updateProcessor(workflowId, node)
            .then(
                resp => {
                    this.props.setWorkflow(resp.data)
                }
            )
    }

    styles = {
        graph: {
            height: '90vh',
            width: '100%'

        }
    }

    render() {
        const nodes = this.props.graph.nodes;
        const edges = this.props.graph.edges;
        const workflowId = this.props.graph.id
        const selected = {};

        const NodeTypes = GraphConfig.NodeTypes;
        const NodeSubtypes = GraphConfig.NodeSubtypes;
        const EdgeTypes = GraphConfig.EdgeTypes;

        return (
            <div id='graph'
                style={this.styles.graph}>

                <GraphView ref='GraphView'
                    nodeKey={NODE_KEY}
                    nodes={nodes}
                    edges={edges}
                    selected={selected}
                    nodeTypes={NodeTypes}
                    nodeSubtypes={NodeSubtypes}
                    edgeTypes={EdgeTypes}
                    allowMultiselect={true} // true by default, set to false to disable multi select.
                    onSelect={this.onSelect}
                    onCreateNode={this.onCreateNode}
                    onUpdateNode={(node) => this.onUpdateNode(node)}
                    onDeleteNode={this.onDeleteNode}
                    onCreateEdge={(source, target) => this.onCreateEdge(source, target)}
                    onSwapEdge={this.onSwapEdge}
                    onDeleteEdge={this.onDeleteEdge}
                    onBackgroundClick={this.onBackgroundClick}
                />
            </div>
        );
    }

}

export default Graph;