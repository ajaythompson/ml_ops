import { makeStyles } from '@material-ui/core/styles';
import Draggable from 'react-draggable';


export default function Canvas(props) {

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
        depBox: {
            position: 'absolute',
            height: 5,
            width: 10,
            backgroundColor: theme.palette.background.paper,
            border: '2px solid #000',
            boxShadow: theme.shadows[5],
            padding: theme.spacing(2, 4, 3),
        },
        noCursor: {
            cursor: "auto"
        },
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

    function getProcessors(processors) {
        if (processors.length > 0) {
            return processors.map(
                processor => {
                    return (
                        <Draggable bounds="parent">
                            <div className={classes.box}>
                                {processor.type}
                                <strong className={classes.noCursor}>
                                    Can't drag here
                                </strong>
                            </div>
                        </Draggable>

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
        <div className={classes.box} style={{ height: '500px', width: '500px', position: 'relative', overflow: 'auto', padding: '0' }}>
            <div style={{ height: '1000px', width: '1000px', padding: '10px' }}>
                {getWorkflow(workflow)}
            </div>
        </div>
    )

}