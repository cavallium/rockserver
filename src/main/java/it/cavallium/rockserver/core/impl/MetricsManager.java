package it.cavallium.rockserver.core.impl;

import io.github.mweirauch.micrometer.jvm.extras.ProcessMemoryMetrics;
import io.github.mweirauch.micrometer.jvm.extras.ProcessThreadMetrics;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmCompilationMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.ipc.http.HttpUrlConnectionSender;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.reactivex.rxjava3.core.Completable;
import io.vertx.core.MultiMap;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.RequestOptions;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.http.HttpClient;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);
	private static final long CLOSE_TIMEOUT_SECONDS = 5L;

	private final JvmGcMetrics gcMetrics;
	private final JvmHeapPressureMetrics heapPressureMetrics;
	private final CompositeMeterRegistry compositeRegistry;
	private final long startTime;
 private HttpClient httpClient;
 private Vertx vertx;

	public MetricsManager(DatabaseConfig config) {
		var resolvedConfig = requireArgument(config, "config");
		try {
			this.startTime = System.currentTimeMillis();
			compositeRegistry = new CompositeMeterRegistry();
			if (resolvedConfig.metrics().jmx().enabled()) {
				try {
					JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(new JmxConfig() {

						@Override
						public @NotNull String prefix() {
							return "rocksdb-jmx";
						}

						@Override
						public @NotNull String domain() {
							return "rockserver.metrics";
						}

						@Override
						public String get(@NotNull String s) {
							return null;
						}
					}, Clock.SYSTEM);
					compositeRegistry.add(jmxMeterRegistry);
				} catch (Throwable ex) {
					LOG.error("Failed to initialize jmx metrics");
				}
			}
   if (resolvedConfig.metrics().influx().enabled()) {
                try {
                    preloadClasses();

                    // Create a dedicated Vertx instance for metrics HTTP client
                    this.vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(false));
                    this.httpClient = vertx.createHttpClient(new HttpClientOptions()
                            .setTrustAll(resolvedConfig.metrics().influx().allowInsecureCertificates())
                            .setVerifyHost(!resolvedConfig.metrics().influx().allowInsecureCertificates())
                            .setDecompressionSupported(true)
                            .setProtocolVersion(HttpVersion.HTTP_2)
                            .setUseAlpn(true)
                            .setConnectTimeout(1000)
                            .setReadIdleTimeout(10000));
					var influxUrl = resolvedConfig.metrics().influx().url();
					var bucket = resolvedConfig.metrics().influx().bucket();
					var userName = resolvedConfig.metrics().influx().user();
					var token = resolvedConfig.metrics().influx().token();
					var org = resolvedConfig.metrics().influx().org();
					var step = Duration.ofMinutes(1);
					InfluxMeterRegistry influxMeterRegistry = InfluxMeterRegistry
							.builder(new InfluxConfig() {

								@Override
								public String uri() {
									return influxUrl;
								}

								@Override
								public String bucket() {
									return bucket;
								}

								@Override
								public String userName() {
									return userName;
								}

								@Override
								public String token() {
									return token;
								}

								@Override
								public String org() {
									return org;
								}

								@Override
								public Duration step() {
									return step;
								}

								@Override
								public String get(@NotNull String s) {
									return null;
								}
							})
							.clock(Clock.SYSTEM)
							.httpClient(new HttpUrlConnectionSender() {

								@Override
								public Response send(Request request) {
									if (httpClient == null) {
										return new Response(400, "httpClient is null");
									}
									Method method = request.getMethod();
									var requestOptions = new RequestOptions();
									requestOptions.setMethod(HttpMethod.valueOf(method.name()));
									requestOptions.setAbsoluteURI(request.getUrl());
									requestOptions.setTimeout(10000);
									MultiMap headers = MultiMap.caseInsensitiveMultiMap();
									for (Entry<String, String> header : request.getRequestHeaders().entrySet()) {
										headers.add(header.getKey(), header.getValue());
									}
									requestOptions.setHeaders(headers);
									return httpClient
											.rxRequest(requestOptions).flatMap(req -> {
												if (method != Method.GET) {
													return req.rxSend(Buffer.buffer(request.getEntity()));
												} else {
													return req.rxSend();
												}
											})
											.flatMap(response -> {
												int status = response.statusCode();

												return response.rxBody()
														.map(body -> new Response(status, body.toString(StandardCharsets.UTF_8)))
														.onErrorReturn(ex -> new Response(status, ex.toString()));
											})
											.onErrorReturn(ex -> new Response(400, ex.toString()))
											.blockingGet();
								}
							})
							.threadFactory(r -> {
								Thread t = new Thread(r, "influx-metrics-publisher");
								t.setDaemon(true);
								return t;
							})
							.build();
					compositeRegistry.add(influxMeterRegistry);
    } catch (Throwable ex) {
                    LOG.error("Failed to initialize influx metrics");
                }
            } else {
                this.httpClient = null;
                this.vertx = null;
            }

			var databaseName = resolvedConfig.metrics().databaseName();
			if (databaseName == null) {
				databaseName = "default";
			}
			compositeRegistry.config().commonTags("appname", "rockserver", "database-name", databaseName);

			new JvmCompilationMetrics().bindTo(compositeRegistry);
			new JvmMemoryMetrics().bindTo(compositeRegistry);
			new JvmInfoMetrics().bindTo(compositeRegistry);
			new ProcessorMetrics().bindTo(compositeRegistry);
			new ClassLoaderMetrics().bindTo(compositeRegistry);
			new FileDescriptorMetrics().bindTo(compositeRegistry);
			new UptimeMetrics().bindTo(compositeRegistry);
			this.gcMetrics = new JvmGcMetrics();
			gcMetrics.bindTo(compositeRegistry);
			new JvmThreadMetrics().bindTo(compositeRegistry);
			this.heapPressureMetrics = new JvmHeapPressureMetrics();
			heapPressureMetrics.bindTo(compositeRegistry);
			new ProcessMemoryMetrics().bindTo(compositeRegistry);
			new ProcessThreadMetrics().bindTo(compositeRegistry);

			compositeRegistry.gauge("yotsuba.uptime.millis",
					this,
					statsManager -> System.currentTimeMillis() - statsManager.getStartTime()
			);
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Failed to parse metrics configuration", e);
		}
	}

	private void preloadClasses() {
        try {
					// Preload file deletion method, used during shutdown
					var vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(false));
					try {
						var fs = vertx.fileSystem();
						var tmpDir = fs.createTempDirectoryBlocking("tmp");
						fs.deleteRecursiveBlocking(tmpDir);
					} finally {
						awaitClose(vertx.close(), "preload Vertx instance");
					}
        } catch (Throwable t) {
            // best-effort; this is only to trigger classloading
            LOG.debug("Failed to preload classes", t);
        }
        // Preload rainbowgum logging classes so that replacing the JAR at runtime
        // does not cause ClassNotFoundException for lazily-loaded internal classes
        // (e.g. io.jstach.rainbowgum.Internal$StringBuilderWriter)
        preloadPackageClasses(
            "io.jstach.rainbowgum",
            "it.cavallium.rockserver",
            "it.cavallium.buffer",
            "io.vertx",
            "io.micrometer",
            "org.rocksdb",
            "reactor",
            "org.cliffc.high_scale_lib",
            "it.unimi.dsi",
            "org.reactivestreams",
            "io.github.mweirauch",
            "com.google.common",
						"org.github.gestalt",
						"com.google.protobuf"
        );
    }

    private void preloadPackageClasses(String... packagePrefixes) {
        for (String prefix : packagePrefixes) {
            String resourcePath = prefix.replace('.', '/');
            try {
                var cl = Thread.currentThread().getContextClassLoader();
                if (cl == null) cl = MetricsManager.class.getClassLoader();
                var resources = cl.getResources(resourcePath);
                while (resources.hasMoreElements()) {
                    var url = resources.nextElement();
                    if ("jar".equals(url.getProtocol())) {
                        var jarPath = url.getPath();
                        int sep = jarPath.indexOf('!');
                        if (sep != -1) {
                            jarPath = jarPath.substring(0, sep);
                        }
                        if (jarPath.startsWith("file:")) {
                            jarPath = jarPath.substring(5);
                        }
                        try (var jarFile = new java.util.jar.JarFile(jarPath)) {
                            var entries = jarFile.entries();
                            while (entries.hasMoreElements()) {
                                var entry = entries.nextElement();
                                var name = entry.getName();
                                if (name.startsWith(resourcePath + "/") && name.endsWith(".class")) {
                                    var className = name.replace('/', '.').substring(0, name.length() - 6);
                                    try {
                                        Class.forName(className, true, cl);
                                    } catch (Throwable ignored) {
                                        // some classes may fail to initialize; that's fine
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.debug("Failed to preload classes for package {}", prefix, t);
            }
        }
    }

	private long getStartTime() {
		return startTime;
	}

	@Override
	public void close() {
		if (httpClient != null) {
			try {
				awaitClose(httpClient.rxClose(), "metrics HTTP client");
			} catch (Throwable t) {
				LOG.warn("Failed to close http client for metrics", t);
			}
		}
		if (vertx != null) {
			try {
				awaitClose(vertx.rxClose(), "metrics Vertx instance");
			} catch (Throwable t) {
				LOG.warn("Failed to close Vertx instance for metrics", t);
			}
		}
		gcMetrics.close();
		heapPressureMetrics.close();
		compositeRegistry.close();
	}

	public MeterRegistry getRegistry() {
		return compositeRegistry;
	}

	static void awaitClose(Completable closeOperation, String resourceName) {
		awaitClose(closeOperation, resourceName, CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	@SuppressWarnings("SameParameterValue")
	static void awaitClose(Completable closeOperation, String resourceName, long timeout, TimeUnit unit) {
		requireArgument(closeOperation, "closeOperation");
		requireArgument(resourceName, "resourceName");
		requireArgument(unit, "unit");
		if (timeout <= 0) {
			throw new IllegalArgumentException("timeout must be positive");
		}
		try {
			if (!closeOperation.blockingAwait(timeout, unit)) {
				throw new IllegalStateException(resourceName + " close did not complete within timeout");
			}
		} catch (RuntimeException e) {
			restoreInterruptedStatus(e);
			throw e;
		}
	}

	private static void restoreInterruptedStatus(Throwable failure) {
		var current = failure;
		while (current != null) {
			if (current instanceof InterruptedException) {
				Thread.currentThread().interrupt();
				return;
			}
			current = current.getCause();
		}
	}

	private static <T> T requireArgument(T value, String name) {
		if (value == null) {
			throw new IllegalArgumentException(name + " must not be null");
		}
		return value;
	}
}
