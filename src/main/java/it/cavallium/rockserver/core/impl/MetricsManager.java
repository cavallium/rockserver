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
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.ipc.http.HttpUrlConnectionSender;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
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
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarFile;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);

	private static final String REDACTED = "<redacted>";

	/** The influx token, user and the user info (and its parts) of the url, as far as they can be read. */
	private static java.util.List<String> influxSecrets(DatabaseConfig config) {
		var secrets = new ArrayList<String>();
		try {
			var influx = config.metrics().influx();
			try {
				secrets.add(influx.token());
			} catch (Throwable ignored) {
			}
			try {
				secrets.add(influx.user());
			} catch (Throwable ignored) {
			}
			try {
				var url = influx.url();
				if (url != null) {
					int scheme = url.indexOf("://");
					int authority = scheme < 0 ? 0 : scheme + 3;
					int end = authority;
					while (end < url.length() && "/?#".indexOf(url.charAt(end)) < 0) {
						end++;
					}
					int at = url.lastIndexOf('@', end - 1);
					if (at > authority) {
						var userInfo = url.substring(authority, at);
						secrets.add(userInfo);
						for (var part : userInfo.split(":", -1)) {
							secrets.add(part);
						}
					}
				}
			} catch (Throwable ignored) {
			}
		} catch (Throwable ignored) {
		}
		return secrets;
	}

	/**
	 * A copy of the failure (same class name, stack and cause chain) whose messages have the given secrets masked, so
	 * that the log of a failed exporter initialisation names the cause without leaking a credential (R60-1, R61-14).
	 */
	static Throwable sanitize(Throwable ex, java.util.List<String> secrets) {
		return sanitize(ex, secrets, 0);
	}

	private static Throwable sanitize(Throwable ex, java.util.List<String> secrets, int depth) {
		var message = ex.getClass().getName();
		if (ex.getMessage() != null) {
			var text = ex.getMessage();
			// longest first, so that "user:pass" is masked as one piece before "user" and "pass"
			var sorted = new ArrayList<>(secrets);
			sorted.sort((a, b) -> Integer.compare(b == null ? 0 : b.length(), a == null ? 0 : a.length()));
			for (var secret : sorted) {
				if (secret != null && !secret.isEmpty()) {
					text = text.replace(secret, REDACTED);
				}
			}
			message += ": " + text;
		}
		var cause = ex.getCause() == null || ex.getCause() == ex || depth > 16 ? null : sanitize(ex.getCause(), secrets, depth + 1);
		var copy = new Throwable(message, cause);
		copy.setStackTrace(ex.getStackTrace());
		return copy;
	}

	private final JvmGcMetrics gcMetrics;
	private final JvmHeapPressureMetrics heapPressureMetrics;
	private final CompositeMeterRegistry compositeRegistry;
	private final long startTime;
	private final AtomicBoolean closed = new AtomicBoolean();
	private HttpClient httpClient;
	private Vertx vertx;

	public MetricsManager(DatabaseConfig config) {
		this(config, LOG);
	}

	/** The logger is a seam for tests: a failed exporter initialisation is reported on it (R61-14). */
	public MetricsManager(DatabaseConfig config, Logger log) {
		try {
			this.startTime = System.currentTimeMillis();
			compositeRegistry = new CompositeMeterRegistry();
			if (config.metrics().jmx().enabled()) {
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
					log.error("Failed to initialize jmx metrics", sanitize(ex, java.util.List.of()));
				}
			}
   if (config.metrics().influx().enabled()) {
                try {
                    preloadClasses();

                    // Create a dedicated Vertx instance for metrics HTTP client
                    this.vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(false));
                    this.httpClient = vertx.createHttpClient(new HttpClientOptions()
                            .setTrustAll(config.metrics().influx().allowInsecureCertificates())
                            .setVerifyHost(!config.metrics().influx().allowInsecureCertificates())
                            .setDecompressionSupported(true)
                            .setProtocolVersion(HttpVersion.HTTP_2)
                            .setUseAlpn(true)
                            .setConnectTimeout(1000)
                            .setReadIdleTimeout(10000));
					var influxUrl = config.metrics().influx().url();
					var bucket = config.metrics().influx().bucket();
					var userName = config.metrics().influx().user();
					var token = config.metrics().influx().token();
					var org = config.metrics().influx().org();
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
                    log.error("Failed to initialize influx metrics", sanitize(ex, influxSecrets(config)));
                }
            } else {
                this.httpClient = null;
                this.vertx = null;
            }

			compositeRegistry.config().commonTags("appname", "rockserver", "database-name", Objects.requireNonNullElse(config.metrics().databaseName(), "default"));
			var nodeName = config.metrics().nodeName();
			if (nodeName != null && !nodeName.isBlank()) {
				compositeRegistry.config().commonTags("node", nodeName);
			}

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
						vertx.close().blockingAwait(5, TimeUnit.SECONDS);
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
                        try (var jarFile = new JarFile(jarPath)) {
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
		if (!closed.compareAndSet(false, true)) {
			return;
		}
		try {
			gcMetrics.close();
		} catch (Throwable t) {
			LOG.warn("Failed to close GC metrics", t);
		}
		try {
			heapPressureMetrics.close();
		} catch (Throwable t) {
			LOG.warn("Failed to close heap pressure metrics", t);
		}
		// Push registries publish one final snapshot from close(). Keep their HTTP
		// transport and Vert.x owner alive until that publish has completed.
		var registries = new ArrayList<MeterRegistry>(compositeRegistry.getRegistries());
		registries.add(compositeRegistry);
		for (var registry : registries) {
			try {
				registry.close();
			} catch (Throwable t) {
				LOG.warn("Failed to close metrics registry", t);
			}
			// close() stops publishing but retains meters and their supplier references.
			for (var meter : registry.getMeters()) {
				try {
					registry.remove(meter);
				} catch (Throwable t) {
					LOG.warn("Failed to remove metrics meter {}", meter.getId(), t);
				}
			}
			if (registry != compositeRegistry) {
				compositeRegistry.remove(registry);
			}
		}
		if (httpClient != null) {
			try {
				httpClient.rxClose().blockingAwait(5, TimeUnit.SECONDS);
			} catch (Throwable t) {
				LOG.warn("Failed to close http client for metrics", t);
			}
		}
		if (vertx != null) {
			try {
				vertx.rxClose().blockingAwait(5, TimeUnit.SECONDS);
			} catch (Throwable t) {
				LOG.warn("Failed to close Vertx instance for metrics", t);
			}
		}
	}

	public MeterRegistry getRegistry() {
		return compositeRegistry;
	}
}
