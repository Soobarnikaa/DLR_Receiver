package com.karix.dlrreceiver.util;

import com.karix.dlrreceiver.util.FlowRouter.Flow;
public class FlowDecision {

    private final Flow flow;
    private final boolean platformRejected;

    public FlowDecision(Flow flow, boolean platformRejected) {
        this.flow = flow;
        this.platformRejected = platformRejected;
    }

    public Flow getFlow() {
        return flow;
    }

    public boolean isPlatformRejected() {
        return platformRejected;
    }
}