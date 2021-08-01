import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';

const useStyles = makeStyles((theme) => ({
    root: {
        position: 'absolute',
        width: '100%',
        maxWidth: 360,
        backgroundColor: theme.palette.background.paper,
        border: '2px solid #000',
        boxShadow: theme.shadows[5],
        padding: theme.spacing(2, 4, 3),
    },
}));

export default function ProcessorList(props) {
    const classes = useStyles();
    const modalStyle = {
        top: `50%`,
        left: `50%`,
        transform: `translate(-50%, -50%)`,
    }
    const mountProcessorConfig = (processorName) => {
        console.log('mounting processor config')
        props.hideFun()
        props.mountProcConfig(processorName)
    }


    return (
        <div style={modalStyle} className={classes.root}>
            <h1>Processors</h1>
            <List component="nav" aria-label="main mailbox folders">
                {props.processors.map((name) => (
                    <ListItem
                        button key={name}
                        onClick={() => mountProcessorConfig(name)}>
                        <ListItemText primary={name} />
                    </ListItem>
                ))}
            </List>
        </div>
    );
}
