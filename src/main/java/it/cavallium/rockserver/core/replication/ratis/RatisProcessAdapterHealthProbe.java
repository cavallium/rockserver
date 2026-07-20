package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;

@FunctionalInterface
public interface RatisProcessAdapterHealthProbe {

    RatisProcessAdapterHealth check(RatisProcessAdapterCommand command) throws IOException;

    static RatisProcessAdapterHealthProbe alwaysReady() {
        return _ -> RatisProcessAdapterHealth.ready();
    }
}
