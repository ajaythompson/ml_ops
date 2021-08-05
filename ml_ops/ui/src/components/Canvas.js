import { makeStyles } from '@material-ui/core/styles';
import Draggable from 'react-draggable';
import React from 'react';
import { addRelation, updateProcessor } from '../api/Workbench';
import { LineTo } from 'react-lineto';

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
            left: "50px",
            right: "10px",
            transform: "skew(-45deg)"
        }
    }));


    const classes = useStyles();
    const workflow = props.workflow

    function getWorkflow() {
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

    function getRelations() {
        if (workflow != null && workflow.relations.length > 0) {
            const processorMap = new Map(workflow.processors.map(i => [i.id, i]))
            const relations = workflow.relations
            console.log(relations)
            return relations.map(
                rel => {
                    const leftProcessor = processorMap.get(rel.left)
                    const rightProcessor = processorMap.get(rel.right)

                    // const x1 = leftProcessor.x_pos
                    // const x2 = rightProcessor.x_pos
                    // const y1 = leftProcessor.y_pos
                    // const y2 = rightProcessor.y_pos

                    // return (
                    //     <div>
                    //         <h1>HELLO</h1>
                    //         <Line x1={x1} y1={y1} x2={x2} y2={y2}
                    //             borderColor="black"
                    //             borderWidth="10%" />
                    //     </div>

                    // )
                    return (
                        <LineTo
                            id={rel.id}
                            from={leftProcessor.id}
                            to={rightProcessor.id}
                            borderStyle="dashed"
                            borderColor="red"
                            borderWidth="2px"
                            delay="0"
                        />

                    )
                }
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
                {getWorkflow()}
                {getRelations()}
            </div>

        </div>

        // <div>
        //     <div className="A">Element A</div>
        //     <div className="B">Element B</div>
        //     <LineTo from="A" to="B" />
        // </div>
    )

}
