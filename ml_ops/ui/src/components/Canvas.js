import React, { useState, useEffect } from 'react';
import dagreD3 from 'dagre-d3';
import * as d3 from 'd3';

const Canvas = () => {




    useEffect(() => {
        var g = new dagreD3.graphlib.Graph({ directed: true });
        // Set an object for the graph label
        g.setGraph({});

        g.graph().rankdir = 'LR';
        g.graph().ranksep = 50;
        g.graph().nodesep = 5;

        // Default to assigning a new object as a label for each new edge.
        g.setDefaultEdgeLabel(function () {
            return {};
        });
        g.setNode('root', {
            label: 'root',
            width: 50,
            height: 20,
            shape: 'ellipse',
            style: 'stroke: black; fill:blue; stroke-width: 1px; ',
            labelStyle: "font: 300 14px 'Helvetica Neue', Helvetica;fill: white;",
        });

        g.setNode('child', {
            label: 'child',
            width: 50,
            height: 20,
            shape: 'ellipse',
            style: 'stroke: black; fill:blue; stroke-width: 1px; ',
            labelStyle: "font: 300 14px 'Helvetica Neue', Helvetica;fill: white;",
        });

        g.setEdge('root', 'child', {
            curve: d3.curveBasis,
            style: 'stroke: gray; fill:none; stroke-width: 1px; stroke-dasharray: 5, 5;',
            arrowheadStyle: 'fill: gray',
        })


        var svg = d3.select('svg'),
            inner = svg.select('g');

        // // Create the renderer
        var render = new dagreD3.render();
        render(inner, g);
    });


    return (
        <div>
            <h1>Decision Tree le lo</h1>
            <svg width="700" height="600">
                <g />
            </svg>
        </div>
    );
}

export default Canvas;