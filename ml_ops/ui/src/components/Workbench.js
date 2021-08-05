import React from 'react';
import AppBar from '@material-ui/core/AppBar'
import Button from '@material-ui/core/Button'
import Modal from '@material-ui/core/Modal'
import Toolbar from '@material-ui/core/Toolbar'

import ProcessorList from './ProcessorList';
import styles from './Workbench.css';
import { getProcessorList, createWorkflow } from '../api/Workbench';
import Canvas from './Canvas';
import ProcessorConfig from './ProcessorConfig'
import { getProcessorDescription } from '../api/Workbench';
import Graph from './Graph';


const data = {
    "edges": [],
    "id": "1c1177fc-f512-11eb-a1f4-00155de8c683",
    "nodes": [
        {
            "id": "1e5a2f54-f512-11eb-b072-00155de8c683",
            "title": "asdf",
            "processor_type": "LoadProcessor",
            "properties": {},
            "type": "node",
            "x": 0,
            "y": 0
        }
    ]
}

export default function Workbench() {

    const [processorListEnabled, setProcessorListEnabled] = React.useState(false)
    const [processors, setProcessors] = React.useState([])
    const [workflow, setWorkflow] = React.useState(null)
    const [mountProcessorConfig, setMountProcessorConfig] = React.useState(false)
    const [processorConfig, setProcessorConfig] = React.useState(null)


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


    const renderWorkflow = (x) => {

        if (workflow == null) {
            return (
                <div>
                    <h1>
                        Kindly create new workflow!
                    </h1>
                </div>)
        } else {
            return (<Graph graph={x} setWorkflow={setWorkflow} />)
        }
    }




    return (
        <div>
            <AppBar position="static">
                <Toolbar>
                    <Button variant="contained"
                        onClick={newWorkflow}>W</Button>
                    <Button variant="contained"
                        onClick={showProcessorList}>P</Button>
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
            {renderWorkflow(workflow)}
        </div>
    )
}