package com.kubling.provider.oracle;

final class RequestValidationException extends IllegalArgumentException {
    RequestValidationException(String message) {
        super(message);
    }

    RequestValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
