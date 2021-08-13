import React from 'react';
import AppBar from '@material-ui/core/AppBar'
import Button from '@material-ui/core/Button'
import Modal from '@material-ui/core/Modal'
import Toolbar from '@material-ui/core/Toolbar'
import Typography from '@material-ui/core/Typography';
import { makeStyles } from '@material-ui/core/styles';


import ProcessorList from './ProcessorList';
import styles from './Workbench.css';
import { getProcessorList, createWorkflow } from '../api/Workbench';
import ProcessorConfig from './ProcessorConfig'
import RelationConfig from './RelationConfig';
import { getProcessorDescription, runWorkflow } from '../api/Workbench';
import Graph from './Graph';

const useStyles = makeStyles((theme) => ({
    menuButton: {
        marginLeft: "auto",
    },
    root: {
        flexGrow: 1,
    },
    paper: {
        "color": "yellow",
        "display": "none", /* Hidden by default */
        "position": "fixed", /* Stay in place */
        "z-index": "1", /* Sit on top */
        "left": "0",
        "top": "0",
        "width": "10%", /* Full width */
        "height": "30%", /* Full height */
        "overflow": "auto", /* Enable scroll if needed */
        "background-color": "rgb(0,0,0)", /* Fallback color */
        "background-color": "rgba(0,0,0,0.4)", /* Black w/ opacity */
    }
}))


export default function Workbench() {

    const classes = useStyles();

    const [processorListEnabled, setProcessorListEnabled] = React.useState(false)
    const [processors, setProcessors] = React.useState([])
    const [workflow, setWorkflow] = React.useState(null)
    const [mountProcessorConfig, setMountProcessorConfig] = React.useState(false)
    const [processorConfig, setProcessorConfig] = React.useState(null)
    const [showRelationConfig, setShowRelationConfig] = React.useState(false)

    // relation states
    const [relationConfig, setRelationConfig] = React.useState(null)
    const [selectedProcessor, setSelectedProcessor] = React.useState(null)
    const [relations, setRelations] = React.useState(null)


    function hideProcessorList() {
        setProcessorListEnabled(false)
    }

    function showProcessorList() {
        getProcessorList().then(response => {
            setProcessors(response.data.processors)
            setProcessorListEnabled(true)
        });
    }

    function hideProcessorConfig() {
        setMountProcessorConfig(false)
    }

    function hideRelationConfig() {
        setShowRelationConfig(false)
    }

    function mountRelationSelector(data) {
        const nodes = workflow.nodes
        const nodes_map = new Map(nodes.map(e => [e.id, e]))
        const processorType = nodes_map.get(data.target).processor_type
        getProcessorDescription(processorType).then(
            resp => {
                setRelations(resp.data.relations.map(x => x.name))
                setRelationConfig(data)
                console.log(relations)
                setShowRelationConfig(true)
            }
        )
    }

    function showProcessorConfig(processorType) {
        getProcessorDescription(processorType)
            .then(
                response => {
                    console.log(response.data)
                    setProcessorConfig(response.data)
                    setMountProcessorConfig(true)
                }
            )
    }

    function newWorkflow() {
        createWorkflow().then(response => {
            console.log(workflow)
            if (workflow == null) {
                setWorkflow(response.data)
                console.log(workflow)
            }
        });
    }

    function execWorkflow() {
        runWorkflow(workflow.id).then(response => console.log("done"))
    }

    function getWorkflowId() {
        if (workflow == null) {
            return "No Workflow Selected!"
        } else {
            return workflow.id
        }
    }

    function getProcessorId() {
        var result = "No processor Selected!"

        if (selectedProcessor != null && "nodes" in selectedProcessor) {
            result = selectedProcessor.nodes.entries().next().value[0]
        }

        return result
    }

    const renderWorkflow = (x) => {

        if (workflow == null) {
            return (
                <div>
                    <h1>
                        Kindly create new workflow!
                    </h1>
                </div>)
        } else {
            return (<Graph graph={x}
                setWorkflow={setWorkflow}
                mountRelationSelector={mountRelationSelector}
                setSelectedProcessor={setSelectedProcessor}
            />)
        }
    }




    return (
        <div className={classes.root}>
            <AppBar position="static">
                <Toolbar>
                    <Button variant="contained"
                        onClick={newWorkflow}>W</Button>
                    <Button variant="contained"
                        onClick={showProcessorList}>P</Button>
                    <Typography variant="title" color="inherit">
                        {getWorkflowId()}
                    </Typography>
                    <div className={classes.menuButton}>
                        <Typography variant="title" color="inherit">
                            {getProcessorId()}
                        </Typography>
                        <Button
                            onClick={execWorkflow}
                            variant="outline"
                            color="inherit">
                            R
                        </Button>
                    </div>

                </Toolbar>
            </AppBar>
            <Modal
                open={processorListEnabled}
                onClose={hideProcessorList}
                className={styles.paper}>
                <ProcessorList
                    workflowId={workflow}
                    processors={processors}
                    hideFun={hideProcessorList}
                    mountProcConfig={showProcessorConfig} />
            </Modal>
            <Modal
                open={mountProcessorConfig}
                onClose={hideProcessorConfig}
                className={styles.paper}>
                <ProcessorConfig
                    config={processorConfig}
                    workflow={workflow}
                    setWorkflow={setWorkflow}
                />
            </Modal>
            <Modal
                open={showRelationConfig}
                onClose={hideRelationConfig}
                className={styles.paper}>
                <RelationConfig
                    config={relationConfig}
                    workflow={workflow}
                    setWorkflow={setWorkflow}
                    relations={relations}
                />
            </Modal>
            {renderWorkflow(workflow)}
        </div>
    )
}