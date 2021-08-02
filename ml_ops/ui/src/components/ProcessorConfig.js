import React, { useRef } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import FormControl from '@material-ui/core/FormControl';
import TextField from '@material-ui/core/TextField';
import Input from '@material-ui/core/Input';
import InputLabel from '@material-ui/core/InputLabel';
import { keys } from '@material-ui/core/styles/createBreakpoints';
import { Button } from '@material-ui/core';
import { addProcessor } from '../api/Workbench';

// import { getProcessorDescription } from '../api/Workbench';

function getModalStyle() {
    const top = 50
    const left = 50

    return {
        top: `${top}%`,
        left: `${left}%`,
        transform: `translate(-${top}%, -${left}%)`,
    };
}

const useStyles = makeStyles((theme) => ({
    paper: {
        position: 'absolute',
        width: 400,
        backgroundColor: theme.palette.background.paper,
        border: '2px solid #000',
        boxShadow: theme.shadows[5],
        padding: theme.spacing(2, 4, 3),
    },
}));


export default function ProcessorConfig(props) {
    const classes = useStyles();
    const [modalStyle] = React.useState(getModalStyle);
    const processorType = props.config.type
    const [properties, setProperties] = React.useState({});

    const handleChange = e => {
        const target = e.target
        const name = target.id
        const value = target.value
        setProperties(prevState => ({
            ...prevState,
            [name]: value
        }));
    };


    const handleSubmit = (event) => {
        // Prevent default behavior
        event.preventDefault();
        const body = {
            "type": processorType,
            "properties": properties
        }
        console.log(body)
        addProcessor(props.workflow.id, body)
            .then(
                resp => props.setWorkflow(resp.data)
            )
    }

    function processorInput(props) {
        return (
            props.map(
                property =>
                    <div key={property.name}>
                        <TextField
                            id={property.name}
                            label={property.name}
                            onChange={handleChange}
                        />
                    </div>
            )

        )
    }


    return (
        <div style={modalStyle} className={classes.paper}>
            <h1>{props.config.type}</h1>
            <form onSubmit={handleSubmit}>
                {processorInput(props.config.properties)}
                <div>
                    <Button type="Submit" variant="contained" color="primary">Ok</Button>
                </div>
            </form>
        </div>
    )
}