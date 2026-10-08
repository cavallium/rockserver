package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.config.ConfigParser;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Optional recovery-only launcher; standalone Main and frontend transports are unchanged. */
public final class RatisNodeMain {
    private RatisNodeMain() {}
    public static void main(String[] args) throws Exception {
        if (args.length!=3) throw new IllegalArgumentException("Usage: RatisNodeMain <established-node-root> <node-manifest> <storage-config>");
        var manifest=RatisNodeManifest.read(Path.of(args[1]));
        try (var service=RatisNodeService.recover(manifest,Path.of(args[0]),ConfigParser.parse(Path.of(args[2])))) {
            var stopped=new CountDownLatch(1); var launched=new CountDownLatch(1);
            var hook=new Thread(() -> {
                try {
                    if (!launched.await(manifest.settings().shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS)) throw new java.io.IOException("Node startup did not settle before shutdown deadline");
                    service.close();
                }
                catch (Exception failure) { System.err.println("Distributed node shutdown failed: "+failure); }
                finally { stopped.countDown(); }
            },"distributed-node-shutdown");
            Runtime.getRuntime().addShutdownHook(hook);
            try {
                try { service.start(); } finally { launched.countDown(); }
                System.out.println("Distributed node running; catalogue/quorum readiness requires separate verification"); stopped.await(); }
            finally { try { Runtime.getRuntime().removeShutdownHook(hook); } catch (IllegalStateException shutdown) { /* JVM shutdown is in progress. */ } }
        }
    }
}
