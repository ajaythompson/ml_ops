import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Modal from '@material-ui/core/Modal';
import { getProcessors, getProcessor } from '../api/WorkflowAPI';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import { ListItem } from '@material-ui/core';

function rand() {
  return Math.round(Math.random() * 20) - 10;
}

function getModalStyle() {
  const top = 50 + rand();
  const left = 50 + rand();

  return {
    top: `${top}%`,
    left: `${left}%`,
    transform: `translate(-${top}%, -${left}%)`,
    width: "25%",
    height: "25%",
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
  table: {
    minWidth: 150,
    maxHeight: 150,
  },
}));

export default function ProcessorsModal() {
  const classes = useStyles();
  // getModalStyle is not a pure function, we roll the style only on the first render
  const [modalStyle] = React.useState(getModalStyle);
  const [open, setOpen] = React.useState(false);
  const [processorList, setProcessorList] = React.useState([]);
  const [propertiesList, setPropertiesList] = React.useState([]);
  const [processorOpen, setProcessorOpen] = React.useState(false);
  // let processorList = [1, 2, 3]

  const handleOpen = () => {
    getProcessors()
      .then(res => {
        setProcessorList(res.data.processor_list)
        console.log(processorList)
      })
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

  const handleProcessorOpen = (name) => {
    handleClose();
    getProcessor(name)
      .then(res => {
        setPropertiesList(res.data.properties_schema.properties)
        console.log(processorList)
      })
    setProcessorOpen(true);
  };

  const handleProcessorClose = () => {
    setProcessorOpen(false);
  };

  const body2 = (
    <div style={modalStyle} className={classes.paper}>
      <ul>
        {
          processorList.map(item => <li>{item}</li>)
        }
      </ul>
    </div>
  );

  const processorModal = (
    <TableContainer style={modalStyle} className={classes.paper}>
      <Table className={classes.table} aria-label="simple table" size='small' stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Processor</TableCell>
          </TableRow>
        </TableHead>
      </Table>
      <TableBody>
      {propertiesList.map((key, value) => (
            <TableRow key={ListItem}>
              <TableCell component="th" scope="row">
                  {key}
              </TableCell>
            </TableRow>
          ))}
      </TableBody>
    </TableContainer>
  );

  const body = (
    <TableContainer style={modalStyle} className={classes.paper}>
      <Table className={classes.table} aria-label="simple table" size='small' stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Processors</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
        {processorList.map((name) => (
            <TableRow key={ListItem}>
              <TableCell component="th" scope="row">
                <button onClick={handleProcessorOpen}>
                  {name}
                </button>

              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );

  return (
    <div>
      <button color="inherit" onClick={handleOpen}>
        Processors
      </button>
      <Modal
        open={open}
        onClose={handleClose}
        aria-labelledby="simple-modal-title"
        aria-describedby="simple-modal-description"
      >
        {body}
      </Modal>
      <Modal
        open={processorOpen}
        onClose={handleProcessorClose}
        aria-labelledby="simple-modal-title"
        aria-describedby="simple-modal-description"
      >
        {processorModal}
      </Modal>
    </div>
  );
}
