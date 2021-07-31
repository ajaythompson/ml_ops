import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';

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

export default function ProcessorList(props) {
    const classes = useStyles();
    const [modalStyle] = React.useState(getModalStyle);
    // const [showProcessor, setShowProcessor] = React.useState(false)


    return (
        <div style={modalStyle} className={classes.paper}>
            <h2 id="simple-modal-title">Processors</h2>
            <List>
                <TableContainer style={modalStyle} className={classes.paper}>
                    <Table className={classes.table}
                        aria-label="simple table" size='small' stickyHeader>
                        <TableHead>
                            <TableRow>
                                <TableCell>Processors</TableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {props.processors.map((name) => (
                                <TableRow key={ListItem}>
                                    <TableCell component="th" scope="row">
                                        <button onClick={props.hideFun}>
                                            {name}
                                        </button>

                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </TableContainer>
            </List>
        </div>
    );
}
