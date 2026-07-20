package it.cavallium.rockserver.core.server;

import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.replication.ratis.RatisServerLifecycle;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;

public class ServerBuilder {

    private HostAndPort iNetAddress;
    private UnixDomainSocketAddress unixAddress;
    private HostAndPort http2Address;
    private boolean useThrift;
    private RocksDBConnection client;
    private RatisServerLifecycle ratisLifecycle;

    public void setUnixSocket(UnixDomainSocketAddress address) {
        this.unixAddress = address;
    }

    public void setAddress(HostAndPort address) {
        this.iNetAddress = address;
    }

    public void setHttpAddress(HostAndPort address) {
        this.http2Address = address;
    }

    public void setUseThrift(boolean useThrift) {
        this.useThrift = useThrift;
    }

    public void setClient(RocksDBConnection client) {
        this.client = client;
    }

    public void setRatisLifecycle(RatisServerLifecycle ratisLifecycle) {
        this.ratisLifecycle = ratisLifecycle;
    }

    public Server build() throws IOException {
        if (http2Address != null) {
            if (useThrift) {
                return new ThriftServer(client, http2Address.host(), http2Address.port(), ratisLifecycle);
            } else {
                return new GrpcServer(client, new InetSocketAddress(http2Address.host(), http2Address.port()),
                        ratisLifecycle);
            }
        } else if (unixAddress != null) {
            return new GrpcServer(client, new DomainSocketAddress(unixAddress.getPath().toFile()), ratisLifecycle);
        } else if (iNetAddress != null) {
            if (useThrift) {
                throw new IllegalArgumentException("Thrift servers do not support inet addresses");
            } else {
                return new GrpcServer(client, new InetSocketAddress(iNetAddress.host(), iNetAddress.port()),
                        ratisLifecycle);
            }
        } else {
            throw new IllegalStateException("Please set a connection type");
        }
    }
}
