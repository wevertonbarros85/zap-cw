import React from "react";
import withWidth, { isWidthUp } from '@material-ui/core/withWidth';

import Tickets from "../Tickets"

function TicketResponsiveContainer (props) {
     if (isWidthUp('md', props.width)) {
        return <Tickets />;    
    }
    return <Tickets />
}

export default withWidth()(TicketResponsiveContainer);