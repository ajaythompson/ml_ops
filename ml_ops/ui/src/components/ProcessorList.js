import React from 'react';
import Modal from '@material-ui/core/Modal';
import { getProcessor, getProcessorList } from '../api/WorkflowAPI';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import { ListItem } from '@material-ui/core';
import Processor from './ProcessorModal';
import { getModalStyle, useStyles } from './Styles';



export function ProcessorList() {
  const classes = useStyles();
  // getModalStyle is not a pure function, we roll the style only 
  // on the first render
  const [modalStyle] = React.useState(getModalStyle);
  const [processorListOpen, setProcessorListOpen] = React.useState(false);
  const [processorList, setProcessorList] = React.useState([]);
  const [processorOpen, setProcessorOpen] = React.useState(false);
  const [processorName, setProcessorName] = React.useState('');
  const [processor, setProcessor] = React.useState({});

  const handleProcessorListOpen = () => {
    getProcessorList()
      .then(res => {
        setProcessorList(res.data.processor_list)
        console.log(processorList)
      })
    setProcessorListOpen(true);
  };

  const handleProcessorListClose = () => {
    setProcessorListOpen(false);
  };

  const handleProcessorOpen = (name) => {
    setProcessorListOpen(false);
    setProcessorName(name);
    setProcessorOpen(true);
    getProcessor(name)
      .then(
        res => {
          setProcessor(res.data)
        }
      )
    console.log(processor)
  };

  const handleProcessorClose = () => {
    setProcessorOpen(false);
  };


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
                <button onClick={() => {
                  handleProcessorOpen(name);
                }}>
                  {name}
                </button>

              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );

  const proc = (name) => {
    return (
      <div style={modalStyle} className={classes.paper}>
        name: {name}
        description: {processor.description}
      </div>)
  }

  return (
    <div>
      <button color="inherit" onClick={handleProcessorListOpen}>
        Processors
      </button>
      <Modal
        open={processorListOpen}
        onClose={handleProcessorListClose}
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
        {/* {proc(processorName)} */}
        {Processor(processorName, processor)}
      </Modal>
    </div>
  );
}
