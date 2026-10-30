package it.cavallium.rockserver.core.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closes a connection when the server sends nothing for this long after it was established.
 *
 * <p>The kernel completes the connections of a frozen process (SIGSTOP, a long pause) that still listens, so the
 * connect timeout does not see it, and gRPC would wait for the HTTP/2 settings of the server until keepalive gives up.
 * The channel then moves to its next address at once, instead of after keepalive's time and timeout. This handler sits
 * at the head of the pipeline and removes itself on the first bytes received.</p>
 *
 * <p>A hung disk is not detected: the process stops serving its database, but its event loops still answer the HTTP/2
 * handshake and keepalive.</p>
 */
final class ServerGreetingTimeoutHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(ServerGreetingTimeoutHandler.class);

	private final long timeoutMillis;
	private ScheduledFuture<?> timeout;

	ServerGreetingTimeoutHandler(long timeoutMillis) {
		this.timeoutMillis = timeoutMillis;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) {
		timeout = ctx.executor().schedule(() -> {
			LOG.warn("{} sent nothing for {} ms after the connection, closing it", ctx.channel().remoteAddress(),
					timeoutMillis);
			ctx.close();
		}, timeoutMillis, TimeUnit.MILLISECONDS);
		ctx.fireChannelActive();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		cancelTimeout();
		ctx.pipeline().remove(this);
		ctx.fireChannelRead(msg);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) {
		cancelTimeout();
		ctx.fireChannelInactive();
	}

	private void cancelTimeout() {
		if (timeout != null) {
			timeout.cancel(false);
			timeout = null;
		}
	}
}
