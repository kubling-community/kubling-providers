package com.kubling.provider.oracle;

import java.util.LinkedHashSet;
import java.util.Set;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

final class XaSupport {
    private XaSupport() {}

    static Set<BranchXid> recover(XAResource resource) throws XAException {
        Set<BranchXid> recovered = new LinkedHashSet<>();
        boolean scanStarted = false;
        try {
            Xid[] batch = resource.recover(XAResource.TMSTARTRSCAN);
            scanStarted = true;
            while (batch != null && batch.length > 0) {
                for (Xid xid : batch) {
                    recovered.add(new BranchXid(
                            xid.getFormatId(),
                            xid.getGlobalTransactionId(),
                            xid.getBranchQualifier()));
                }
                batch = resource.recover(XAResource.TMNOFLAGS);
            }
        } finally {
            if (scanStarted) {
                resource.recover(XAResource.TMENDRSCAN);
            }
        }
        return Set.copyOf(recovered);
    }

    static BranchState heuristicState(XAException exception) {
        return switch (exception.errorCode) {
            case XAException.XA_HEURCOM -> BranchState.HEURISTIC_COMMIT;
            case XAException.XA_HEURRB -> BranchState.HEURISTIC_ROLLBACK;
            case XAException.XA_HEURMIX -> BranchState.HEURISTIC_MIXED;
            case XAException.XA_HEURHAZ -> BranchState.HEURISTIC_HAZARD;
            default -> null;
        };
    }
}
