import { makeStyles } from '@material-ui/core/styles';
import Draggable from 'react-draggable';


export default function Canvas(props) {

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
            cursor: "auto",
            width: 30,
            height: 80,
            float: 'left',
            textAlignLast: 'left',
            // padding: '20px',
            border: '2px solid red'
        },
        small: {
            width: 1,
            height: 1,
            float: 'left',
            padding: '20px',
            border: '2px solid red'
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
        console.log('dragging')
        // ev.dataTransfer.setData("text", ev.target.id);
    }

    function allowDrop(event) {
        event.preventDefault();
    }

    function drop(event) {
        console.log(event)
    }

    function getProcessors(processors) {
        if (processors.length > 0) {
            return processors.map(
                processor => {
                    return (
                        <Draggable bounds="parent" cancel="strong">
                            <div className={classes.box}
                                onDrop={drop}
                                ondragover={allowDrop}>
                                <div className={classes.child}>
                                    {processor.type}
                                </div>

                                <strong className={classes.noCursor}>
                                    <div id='id1'
                                        draggable='true'
                                        onDragStart={drag}
                                        className={{ 'float': 'center' }} >O</div>
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
            height: '500px', width: '500px',
            // position: 'relative',
            overflow: 'auto', padding: '0'
        }}
            onDrop={drop}
            ondragover={allowDrop}>
            <div className={classes.child} id="div1" ondrop="drop(event)" ondragover="allowDrop(event)"></div>
            <div className={classes.small} id="drag1" draggable="true"
                ondragstart={drag}>2</div>
            <div style={{ height: '1000px', width: '1000px', padding: '10px' }}
                onDrop={drop}
                ondragover={allowDrop}>
                {getWorkflow(workflow)}
            </div>

        </div>
    )

}