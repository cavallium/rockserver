package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.config.ConfigParser;
import java.nio.file.Path;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Established distributed node with an owned common API frontend. */
public final class RatisNodeMain {
    private RatisNodeMain() {}
    public static void main(String[] args) throws Exception {
        if (args.length!=6) throw new IllegalArgumentException("Usage: RatisNodeMain <established-node-root> <node-manifest> <storage-config> <backend-root> <grpc://host:port|thrift://host:port> <new-column-domain-uuid>");
        var listen=URI.create(args[4]);RatisNodeService.requireListenAddress(listen);
        var placement=UUID.fromString(args[5]);
        var manifest=RatisNodeManifest.read(Path.of(args[1]));manifest.plan().domain(placement);
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
                try { service.start();service.startFrontend(Path.of(args[3]),listen,(name,schema) -> placement,(name,className,sha,revision) -> {throw new java.io.IOException("No trusted operator resolver configured");},RatisDatabaseBackendFactory.Policy.defaults()); } finally { launched.countDown(); }
                System.out.println("Distributed node frontend listening at "+listen); stopped.await(); }
            finally { try { Runtime.getRuntime().removeShutdownHook(hook); } catch (IllegalStateException shutdown) { /* JVM shutdown is in progress. */ } }
        }
    }
}
