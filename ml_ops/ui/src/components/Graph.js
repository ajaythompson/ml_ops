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
import Modal from '@material-ui/core/Modal'
import React from 'react';
import { addRelation, updateProcessor } from '../api/Workbench'
import ProcessorOption from './ProcessorOption';
import styles from './Workbench.css';

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

        this.state = {
            "mountProcessorOptions": false
        }
    }

    /* Define custom graph editing methods here */
    onCreateNode() {
        this.setState({})
        // console.log(this.state)
    }

    onSelect(node) {
        console.log('selecting')
        this.setState(
            prevState => ({
                ...prevState,
                "selected": node,
                "mountProcessorOptions": true
            })
        )
        console.log(this.state)
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

    unMountProcessorOptions() {
        this.setState(
            prevState => ({
                ...prevState,
                "mountProcessorOptions": false
            })
        )
    }

    styles = {
        graph: {
            height: '90vh',
            width: '100%'

        },
        paper: {
            position: 'absolute',
            width: 400,
            border: '2px solid #000',
        },
        root: {
            position: 'absolute',
            width: '100%',
            maxWidth: 360,
            // backgroundColor: theme.palette.background.paper,
            border: '2px solid #000',
            // boxShadow: theme.shadows[5],
            // padding: theme.spacing(2, 4, 3),
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
                    onSelect={(node) => this.onSelect(node)}
                    onCreateNode={this.onCreateNode}
                    onUpdateNode={(node) => this.onUpdateNode(node)}
                    onDeleteNode={this.onDeleteNode}
                    onCreateEdge={(source, target) => this.onCreateEdge(source, target)}
                    onSwapEdge={this.onSwapEdge}
                    onDeleteEdge={this.onDeleteEdge}
                    onBackgroundClick={this.onBackgroundClick}
                />
                <Modal
                    open={this.state.mountProcessorOptions}
                    onClose={() => this.unMountProcessorOptions()}
                    className={this.styles.root}
                >
                    <ProcessorOption
                        className={this.styles.root}
                    />
                </Modal>
            </div>
        );
    }

}

export default Graph;