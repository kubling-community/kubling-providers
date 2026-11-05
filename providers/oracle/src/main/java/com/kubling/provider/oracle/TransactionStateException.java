package com.kubling.provider.oracle;

final class TransactionStateException extends IllegalStateException {
    TransactionStateException(String message) {
        super(message);
    }
}
