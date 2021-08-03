import { makeStyles } from '@material-ui/core/styles';
import Draggable from 'react-draggable';
import React from 'react';
import { addRelation, updateProcessor } from '../api/Workbench';


export default function Canvas(props) {

    const [dragRelation, setDragRelation] = React.useState(null)

    const useStyles = makeStyles((theme) => ({
        box: {
            // position: 'absolute',
            height: 80,
            width: 170,
            // backgroundColor: theme.palette.background.paper,
            // border: '2px solid #000',
            // boxShadow: theme.shadows[5],
            // padding: theme.spacing(2, 4, 3),
            border: '3px solid #fff',
            // padding: '2px'
        },
        child: {
            width: 90,
            height: 40,
            float: 'left',
            padding: '20px',
            border: '2px solid red'

        },
        noCursor: {
            // cursor: "auto",
            width: 30,
            height: 80,
            float: 'left',
            textAlignLast: 'left',
            // padding: '20px',
            // border: '2px solid red'
        },
        small: {
            width: 1,
            height: 1,
            float: 'left',
            padding: '20px',
            border: '2px solid red'
        },
        line: {
            position: "absolute",
            width: "500px",
            height: "2px",
            "background-color": "red",
            top: "100px",
            left: "50px"
        }
    }));


    const classes = useStyles();
    const workflow = props.workflow

    function getWorkflow(workflow) {
        if (workflow != null) {
            return getProcessors(workflow.processors)
        }
        else {
            return (
                <div />
            )
        }
    }

    function drag(ev) {
        console.log(ev.target.id)
        setDragRelation(ev.target.id)
        // ev.dataTransfer.setData("text", ev.target.id);
    }

    function allowDrop(event) {
        event.preventDefault();
    }

    function drop(event) {
        console.log(event)
        const left = dragRelation
        const right = event.target.id
        const workflowId = props.workflow.id

        const data = {
            'left': left,
            'right': right,
        }

        addRelation(workflowId, data).then(
            resp => props.setWorkflow(resp.data)
        )

    }

    function processorDragStop(ev) {
        const processorId = ev.target.id
        console.log(processorId)
        // const workflow = props.workflow
        const processorMap = new Map(workflow.processors.map(i => [i.id, i]))
        console.log(processorMap)
        var processorBody = processorMap.get(processorId)
        processorBody['x_pos'] = ev.x
        processorBody['y_pos'] = ev.y
        const workflowId = workflow.id
        console.log(processorBody)
        updateProcessor(workflowId, processorBody)
            .then(
                resp => props.setWorkflow(resp.data)
            )
        console.log(ev)
    }



    function getProcessors(processors) {
        if (processors.length > 0) {
            return processors.map(
                processor => {
                    return (
                        <Draggable bounds="parent"
                            cancel="strong"
                            defaultPosition={{ x: processor.x_pos, y: processor.y_pos }}
                            // position={null}
                            onStop={processorDragStop}>
                            <div id={processor.id}
                                className={classes.box}
                                onDrop={drop}
                                onDragOver={allowDrop}>
                                <div id={processor.id} className={classes.child}>
                                    {processor.type}
                                </div>

                                <strong className={classes.noCursor}>
                                    <div id={processor.id}
                                        draggable='true'
                                        onDragStart={drag}
                                        className={{ 'float': 'center' }} >{">"}</div>
                                </strong>
                            </div>

                        </Draggable >

                    )
                }
            )
        } else {
            return (
                <div />
            )
        }

    }

    return (
        <div className={classes.box} style={{
            height: '1500px', width: '1500px',
            // position: 'relative',
            overflow: 'auto', padding: '0'
        }}>
            <div style={{ height: '1500px', width: '1500px', padding: '10px' }}>
                {getWorkflow(workflow)}
            </div>

            <svg>
                <circle stroke-width="1px" stroke="red" x1="0" y1="0" x2="100" y2="50   " id="mySVG" />
            </svg>

        </div>
    )

}
