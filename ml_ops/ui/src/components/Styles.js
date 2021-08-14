import { makeStyles } from "@material-ui/core";

const useStyles = makeStyles((theme) => ({
    paper: {
        position: 'absolute',
        width: 100,
        backgroundColor: theme.palette.background.paper,
        border: '2px solid #000',
        boxShadow: theme.shadows[5],
        padding: theme.spacing(2, 4, 3),
    },
}));

export default useStyles;