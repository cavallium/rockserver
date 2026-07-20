package it.cavallium.rockserver.core.replication.ratis;

public enum RatisAdapterBoundaryMode {
    CLASSPATH_ISOLATED_ADAPTER,
    EXTERNAL_PROCESS;

    public boolean usesProductionModulePath() {
        return false;
    }
}
