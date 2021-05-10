import React from 'react';
import { getModalStyle, useStyles } from './Styles';

const Processor = (name, processor) => {
    const classes = useStyles();
    const [modalStyle] = React.useState(getModalStyle);

    return (
        <div style={modalStyle} className={classes.paper}>
            name: {name}
            description: {processor.description}
        </div>)
}

export default Processor;