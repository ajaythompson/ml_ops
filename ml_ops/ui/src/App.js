import './App.css';
import { useState } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { AppBar, Toolbar} from '@material-ui/core';
import { ProcessorsModal } from './components/ProcessorsModal';

const App = () => {


  const useStyles = makeStyles((theme) => ({
    root: {
      flexGrow: 1,
    },
    menuButton: {
      marginRight: theme.spacing(2),
    },
    title: {
      flexGrow: 1,
    },
  }));

  const classes = useStyles();

  const [open, setOpen] = useState(false);

  const handleOpen = () => {
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

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
    };
  }

  const modalStyle = useState(getModalStyle);

  const body = (
    <div style={modalStyle} className={classes.paper}>
      <h2 id="simple-modal-title">Text in a modal</h2>
      <p id="simple-modal-description">
        Duis mollis, est non commodo luctus, nisi erat porttitor ligula.
      </p>
    </div>
  );

  return (
    <div className="App">
      <head>
      </head>

      <body>
        <div className={classes.root}>
          <AppBar position="static">
            <Toolbar>
              <ProcessorsModal />
            </Toolbar>
          </AppBar>
        </div>
      </body>
    </div>
  );
}

export default App;
