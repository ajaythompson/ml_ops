import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import TextField from '@material-ui/core/TextField';
import { Button } from '@material-ui/core';
import { addRelation } from '../api/Workbench';

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


export default function RelationConfig(props) {
    const classes = useStyles();
    const [modalStyle] = React.useState(getModalStyle);
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
            "source": props.config.source,
            "target": props.config.target,
            "relation": properties.relation
        }
        console.log(body)
        addRelation(props.workflow.id, body)
            .then(
                resp => props.setWorkflow(resp.data)
            )
    }

    function connectionInput(props) {
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
            <form onSubmit={handleSubmit}>
                {/* {connectionInput(props.config.options)} */}
                <div key='relation'>
                    <TextField
                        id='relation'
                        label='relation'
                        onChange={handleChange}
                    />
                </div>
                <div>
                    <Button type="Submit" variant="contained" color="primary">Ok</Button>
                </div>
            </form>
        </div>
    )
}