import Processor from "./Processor";


export default function Canvas() {

    return (
        <div>
            <svg width="300" height="1000">
                <g>
                    <rect x="0" y="0" width="100" height="100" fill="grey"></rect>
                    <text x="0" y="50" fill="blue">Hello</text>
                </g>
            </svg>
        </div >
    )
}