import axios from 'axios';

axios.defaults.baseURL = `${process.env.WEBSERVER_URL}/`;
// axios.defaults.baseURL = `http://localhost:5000`;

export const getProcessorList = () => {
    return axios.get('/processors')
}

export const createWorkflow = () => {
    return axios.post('/workflow')
}

export const addProcessor = (workflowId, data) => {
    return axios.put(`/workflow/${workflowId}/processor`, data)
}

export const deleteProcessor = (workflowId, data) => {
    return axios.delete(`/workflow/${workflowId}/processor`, data)
}

export const updateProcessor = (workflowId, data) => {
    return axios.put(`/workflow/${workflowId}/processor`, data)
}

export const getProcessorDescription = (processorName) => {
    return axios.get(`/processor/${processorName}`)
}

export const addRelation = (workflowId, data) => {
    return axios.post(`/workflow/${workflowId}/connection`, data)
}

export const runWorkflow = (workflowId) => {
    return axios.post(`/workflow/${workflowId}/run`)
}