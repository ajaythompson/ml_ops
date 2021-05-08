import React from 'react';
import Table from '@material-ui/core/Table';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import { getProcessor } from '../api/WorkflowAPI';
import { useStyles, getModalStyle } from './ProcessorsModal';
import { Tab, Tabs } from '@material-ui/core';

const Processor = (name) => {

    const classes = useStyles();

    const processor = getProcessor(name);

    const [value, setValue] = React.useState(2);

    const handleChange = (event, newValue) => {
        setValue(newValue);
    };


    return (
        <div style={getModalStyle()} className={useStyles.paper}>
            Hello
            {/* <Tabs
                value={value}
                indicatorColor="primary"
                textColor="primary"
                onChange={handleChange}
                aria-label="disabled tabs example"
            >
                <Tab label="Active" />
                <Tab label="Disabled" disabled />
                <Tab label="Active" />
            </Tabs> */}
        </div>
    )
}

export default Processor;