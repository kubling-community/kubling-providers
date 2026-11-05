package com.kubling.provider.oracle;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.transaction.xa.Xid;

final class BranchXid implements Xid {
    static final int FORMAT_ID = 0x4b55424c;

    private final int formatId;
    private final byte[] globalTransactionId;
    private final byte[] branchQualifier;

    BranchXid(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
        this.formatId = formatId;
        this.globalTransactionId = globalTransactionId.clone();
        this.branchQualifier = branchQualifier.clone();
        if (this.globalTransactionId.length == 0
                || this.globalTransactionId.length > Xid.MAXGTRIDSIZE
                || this.branchQualifier.length == 0
                || this.branchQualifier.length > Xid.MAXBQUALSIZE) {
            throw new IllegalArgumentException("Oracle Xid components exceed XA limits");
        }
    }

    static BranchXid derive(BranchIdentity identity) {
        byte[] manager = identity.resourceManagerId().getBytes(StandardCharsets.UTF_8);
        byte[] branch = identity.branchId().toByteArray();
        return new BranchXid(
                FORMAT_ID,
                digest((byte) 1, manager, branch),
                digest((byte) 2, manager, branch));
    }

    private static byte[] digest(byte domain, byte[] manager, byte[] branch) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(domain);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(manager.length).array());
            digest.update(manager);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(branch.length).array());
            digest.update(branch);
            return digest.digest();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    @Override
    public int getFormatId() {
        return formatId;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return globalTransactionId.clone();
    }

    @Override
    public byte[] getBranchQualifier() {
        return branchQualifier.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Xid xid
                && formatId == xid.getFormatId()
                && Arrays.equals(globalTransactionId, xid.getGlobalTransactionId())
                && Arrays.equals(branchQualifier, xid.getBranchQualifier());
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(formatId);
        result = 31 * result + Arrays.hashCode(globalTransactionId);
        return 31 * result + Arrays.hashCode(branchQualifier);
    }
}
