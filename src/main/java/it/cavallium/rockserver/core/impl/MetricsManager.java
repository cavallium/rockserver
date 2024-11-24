package it.cavallium.rockserver.core.impl;

import io.github.mweirauch.micrometer.jvm.extras.ProcessMemoryMetrics;
import io.github.mweirauch.micrometer.jvm.extras.ProcessThreadMetrics;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Builder;
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
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.RequestOptions;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.http.HttpClient;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map.Entry;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);

	private final JvmGcMetrics gcMetrics;
	private final JvmHeapPressureMetrics heapPressureMetrics;
	private final CompositeMeterRegistry compositeRegistry;
	private final long startTime;
	private HttpClient httpClient;

	public MetricsManager(DatabaseConfig config) {
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
					LOG.error("Failed to initialize jmx metrics");
				}
			}
			if (config.metrics().influx().enabled()) {
				try {
					this.httpClient = Vertx.vertx().createHttpClient(new HttpClientOptions()
							.setTrustAll(config.metrics().influx().allowInsecureCertificates())
							.setVerifyHost(!config.metrics().influx().allowInsecureCertificates())
							.setTryUseCompression(true)
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
							.threadFactory(new NamedThreadFactory("influx-metrics-publisher"))
							.build();
					compositeRegistry.add(influxMeterRegistry);
				} catch (Throwable ex) {
					LOG.error("Failed to initialize influx metrics");
				}
			} else {
				this.httpClient = null;
			}

			compositeRegistry.config().commonTags("appname", "rockserver");

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

	private long getStartTime() {
		return startTime;
	}

	@Override
	public void close() {
		if (httpClient != null) {
			httpClient.rxClose().blockingAwait();
		}
		gcMetrics.close();
		heapPressureMetrics.close();
		compositeRegistry.close();
	}

	public MeterRegistry getRegistry() {
		return compositeRegistry;
	}
}
