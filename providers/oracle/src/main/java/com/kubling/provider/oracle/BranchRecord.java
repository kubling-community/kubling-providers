package com.kubling.provider.oracle;

record BranchRecord(
        BranchIdentity identity,
        BranchXid xid,
        BranchState state,
        String connectionId) {
    BranchRecord withState(BranchState nextState) {
        return new BranchRecord(identity, xid, nextState, connectionId);
    }
}
