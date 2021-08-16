import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import TextField from '@material-ui/core/TextField';
import { Button } from '@material-ui/core';
import { addProcessor } from '../api/Workbench';
import Divider from '@material-ui/core/Divider';

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
    const [dynamicProperty, setDynamicProperty] = React.useState({});
    let dpCounter = 0
    const body = {}

    if (props.propertyValues != null) {
        body.id = props.propertyValues.id
        body.properties = props.propertyValues
    }
    body.processor_type = processorType

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
        body.properties = properties
        addProcessor(props.workflow.id, body)
            .then(
                resp => {
                    props.setWorkflow(resp.data)
                    props.setSelectedProcessor(null)
                }
            )
    }

    const getPropertyValue = (property) => {
        if ("properties" in props.propertyValues && property in props.propertyValues.properties) {
            console.log(props.propertyValues.properties[property])
            return props.propertyValues.properties[property]
        }
        console.log(props)
        return null
    }

    function processorInput(procConfig) {
        return (
            procConfig.map(
                property =>
                    <div key={property.name}>
                        <TextField
                            id={property.name}
                            defaultValue={getPropertyValue(property.name)}
                            label={property.name}
                            onChange={handleChange}
                        />
                    </div>
            )

        )
    }

    const renderDynamicProperty = () => {
        return (
            dynamicProperty.keys().map(
                prop => {
                    const key = dynamicProperty[prop].key
                    const value = dynamicProperty[prop].value
                    return (
                        <div key={prop}>
                            <TextField
                                id={key}
                                defaultValue={key}
                                label={key}
                                onChange={handleChange}
                            />
                        </div>
                    )
                }
            )
        )
    }

    const addDynamicPropertyValue = (id, value) => {
        setDynamicProperty(
            prevState => ({
                ...prevState,
                [`dp${dpCounter}`]: { "key": "key", "value": "value" }
            })
        )
    }

    const handleDynamicKeyChange = (event) => {
        const target = event.target
        const name = target.id
        const value = target.value

        setDynamicProperty(prevState => ({
            ...prevState,
            [name]: value
        }));
    }


    return (
        <div style={modalStyle} className={classes.paper}>
            <h1>{props.config.type}</h1>
            <form onSubmit={handleSubmit}>
                {processorInput(props.config.properties)}
                <Divider />
                <div>
                    <Button variant="contained" color="primary">
                        ADD
                    </Button>
                </div>
                <Divider />
                <div>
                    <Button type="Submit" variant="contained" color="primary">
                        OK
                    </Button>
                </div>
            </form>
        </div>
    )
}