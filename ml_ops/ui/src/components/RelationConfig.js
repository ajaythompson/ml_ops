import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import TextField from '@material-ui/core/TextField';
import { Button } from '@material-ui/core';
import { addRelation } from '../api/Workbench';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import InputLabel from '@material-ui/core/InputLabel';
import FormControl from '@material-ui/core/FormControl';

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
    formControl: {
        margin: theme.spacing(1),
        minWidth: 120,
    }
}));


export default function RelationConfig(props) {
    const classes = useStyles();
    const [modalStyle] = React.useState(getModalStyle);
    const [properties, setProperties] = React.useState({});

    const handleChange = e => {
        const target = e.target
        const name = "relation"
        const value = target.value
        setProperties(prevState => ({
            ...prevState,
            [name]: value
        }));
    };


    const handleSubmit = (event) => {
        // Prevent default behavior
        event.preventDefault();
        console.log(props)
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

    const getMenuItems = () => {
        return props.relations.map(
            relation => {
                return (
                    <MenuItem value={relation}>{relation}</MenuItem>
                )
            }
        )
    }

    return (
        <div style={modalStyle} className={classes.paper}>
            <form onSubmit={handleSubmit} className={classes.formControl}>
                <InputLabel id="demo-simple-select-label">Relation</InputLabel>
                <Select
                    labelId="demo-simple-select-label"
                    id="demo-simple-select"
                    value={props[0]}
                    onChange={handleChange}
                >
                    {getMenuItems()}
                </Select>
                <div>

                    <Button type="Submit" variant="contained" color="primary">Ok</Button>
                </div>
            </form>
        </div>
    )
}