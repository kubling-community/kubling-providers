package com.kubling.provider.oracle;

@FunctionalInterface
interface ConnectionProbe {
    boolean isHealthy() throws Exception;
}
