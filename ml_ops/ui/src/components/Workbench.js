import React from 'react';
import AppBar from '@material-ui/core/AppBar'
import Button from '@material-ui/core/Button'
import Modal from '@material-ui/core/Modal'
import Toolbar from '@material-ui/core/Toolbar'
import { makeStyles } from '@material-ui/core/styles';

import ProcessorList from './ProcessorList';
import styles from './Workbench.css';
import { getProcessorList, createWorkflow } from '../api/Workbench';
import Canvas from './Canvas';
import Draggable from 'react-draggable';
import ProcessorConfig from './ProcessorConfig'
import { getProcessorDescription } from '../api/Workbench';


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

    const onStart = () => {
        this.setState({ activeDrags: ++this.state.activeDrags });
    };

    const onStop = () => {
        this.setState({ activeDrags: --this.state.activeDrags });
    };

    const useStyles = makeStyles((theme) => ({
        box: {
            position: 'absolute',
            height: 50,
            width: 100,
            backgroundColor: theme.palette.background.paper,
            border: '2px solid #000',
            boxShadow: theme.shadows[5],
            padding: theme.spacing(2, 4, 3),
        },
    }));


    const classes = useStyles();


    return (
        <div>
            <AppBar position="static">
                <Toolbar>
                    <Button variant="contained"
                        onClick={newWorkflow}>C</Button>
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
                <ProcessorConfig config={processorConfig} workflow={workflow} />
            </Modal>
            <div className={classes.box} style={{ height: '500px', width: '500px', position: 'relative', overflow: 'auto', padding: '0' }}>
                <div style={{ height: '1000px', width: '1000px', padding: '10px' }}>
                    <Draggable bounds="parent">
                        <div className={classes.box}>
                            Load Processor
                        </div>
                    </Draggable>
                </div>
            </div>
        </div>
    )
}