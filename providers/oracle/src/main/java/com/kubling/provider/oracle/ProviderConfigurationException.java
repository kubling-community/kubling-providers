package com.kubling.provider.oracle;

final class ProviderConfigurationException extends IllegalArgumentException {
    ProviderConfigurationException(String message) {
        super(message);
    }

    ProviderConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
