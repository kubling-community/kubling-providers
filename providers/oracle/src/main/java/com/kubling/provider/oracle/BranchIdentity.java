package com.kubling.provider.oracle;

import com.google.protobuf.ByteString;
import com.kubling.provider.grpc.TransactionBranchId;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

record BranchIdentity(String resourceManagerId, ByteString branchId) {
    private static final int MAX_ID_BYTES = 256;

    BranchIdentity {
        Objects.requireNonNull(resourceManagerId, "resourceManagerId");
        Objects.requireNonNull(branchId, "branchId");
    }

    static BranchIdentity from(TransactionBranchId branch, String expectedResourceManagerId) {
        if (branch == null) {
            throw new RequestValidationException("transaction branch is required");
        }
        String resourceManagerId = branch.getResourceManagerId();
        validateResourceManagerId(resourceManagerId);
        if (!resourceManagerId.equals(expectedResourceManagerId)) {
            throw new RequestValidationException(
                    "resource_manager_id does not match this Oracle resource manager");
        }
        ByteString branchId = branch.getBranchId();
        if (branchId.isEmpty() || branchId.size() > MAX_ID_BYTES) {
            throw new RequestValidationException(
                    "branch_id must contain between 1 and 256 bytes");
        }
        return new BranchIdentity(resourceManagerId, branchId);
    }

    static void validateResourceManagerId(String resourceManagerId) {
        if (resourceManagerId.isEmpty()
                || !resourceManagerId.strip().equals(resourceManagerId)
                || resourceManagerId.getBytes(StandardCharsets.UTF_8).length > MAX_ID_BYTES) {
            throw new RequestValidationException(
                    "resource_manager_id must be a canonical identifier of at most 256 UTF-8 bytes");
        }
    }

    TransactionBranchId toProto() {
        return TransactionBranchId.newBuilder()
                .setResourceManagerId(resourceManagerId)
                .setBranchId(branchId)
                .build();
    }
}
