import axios from 'axios';

// axios.defaults.baseURL = `${process.env.WEBSERVER_URL}/api/v1`;
axios.defaults.baseURL = `http://localhost:5000`;

export const getProcessorList = () => {
    return axios.get('/workflow/processors')
}

export const getProcessor = (name) => {
    return axios.get(`/workflow/processor?name=${name}`)
}
