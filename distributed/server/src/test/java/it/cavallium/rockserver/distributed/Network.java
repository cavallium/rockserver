package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Faulty network between the nodes of a test cluster, made of one {@link LinkProxy} per directed link and port.
 *
 * <p>Every directed link {@code src -> dst} gets its own loopback address {@code 127.0.0.k} ({@code k} from
 * {@value #FIRST_LINK_HOST} upward). For every port that {@code dst} exposes, a proxy listens on
 * {@code 127.0.0.k:port} and forwards to {@code 127.0.0.1:port}, where {@code dst} really listens. Linux routes the
 * whole {@code 127.0.0.0/8} to the loopback interface and lets a socket bind {@code 127.0.0.k:port} while another
 * one listens on {@code 127.0.0.1:port}.</p>
 *
 * <p>Nodes reach each other by host name ({@link #hostname}). Each node's JVM is started with
 * {@code -Djdk.net.hosts.file=}{@link #hostsFile}, which maps every other node's name to the address of the link
 * from this node to it, and its own name to {@code 127.0.0.1}. So whatever node {@code src} opens towards
 * {@code dst} goes through the link {@code src -> dst}, and blocking that link cuts it.</p>
 */
public final class Network implements AutoCloseable {

	public static final int FIRST_LINK_HOST = 10;

	private final List<String> nodeIds;
	private final Map<Link, LinkState> links;
	private final Map<String, Path> hostsFiles;

	private record Link(String src, String dst) {}

	private record LinkState(InetAddress address, List<LinkProxy> proxies) {}

	private Network(List<String> nodeIds, Map<Link, LinkState> links, Map<String, Path> hostsFiles) {
		this.nodeIds = nodeIds;
		this.links = links;
		this.hostsFiles = hostsFiles;
	}

	/**
	 * Starts the proxies of every directed link and writes the hosts files into {@code dir}.
	 *
	 * @param portsByNode the ports each node listens on (on {@code 127.0.0.1}), in node order
	 */
	public static Network start(Map<String, List<Integer>> portsByNode, Path dir) throws IOException {
		Files.createDirectories(dir);
		var nodeIds = List.copyOf(portsByNode.keySet());
		var links = new LinkedHashMap<Link, LinkState>();
		var hostsFiles = new LinkedHashMap<String, Path>();
		var network = new Network(nodeIds, links, hostsFiles);
		try {
			var addresses = linkAddresses(nodeIds.size()).iterator();
			for (String src : nodeIds) {
				for (String dst : nodeIds) {
					if (src.equals(dst)) {
						continue;
					}
					var address = addresses.next();
					var proxies = new ArrayList<LinkProxy>();
					// Register first so that close() releases the proxies already bound if a later bind fails.
					links.put(new Link(src, dst), new LinkState(address, proxies));
					for (int port : portsByNode.get(dst)) {
						proxies.add(new LinkProxy(new InetSocketAddress(address, port),
								new InetSocketAddress(InetAddress.getLoopbackAddress(), port)));
					}
				}
			}
			for (String src : nodeIds) {
				var file = dir.resolve("hosts-" + hostname(src));
				Files.writeString(file, network.hostsFileContent(src));
				hostsFiles.put(src, file);
			}
		} catch (IOException | RuntimeException e) {
			network.close();
			throw e;
		}
		return network;
	}

	/** The addresses of the links between {@code nodeCount} nodes, in the order {@link #start} assigns them. */
	static List<InetAddress> linkAddresses(int nodeCount) {
		int count = nodeCount * (nodeCount - 1);
		if (FIRST_LINK_HOST + count - 1 > 254) {
			throw new IllegalArgumentException("Too many nodes for 127.0.0.0/24 link addresses: " + nodeCount);
		}
		var addresses = new ArrayList<InetAddress>(count);
		for (int i = 0; i < count; i++) {
			try {
				addresses.add(InetAddress.getByAddress(new byte[] {127, 0, 0, (byte) (FIRST_LINK_HOST + i)}));
			} catch (UnknownHostException e) {
				throw new IllegalStateException("Invalid IPv4 address", e);
			}
		}
		return addresses;
	}

	public static String hostname(String nodeId) {
		return "node-" + nodeId;
	}

	/** The file to pass as {@code -Djdk.net.hosts.file} to the JVM of {@code nodeId}. */
	public Path hostsFile(String nodeId) {
		var file = hostsFiles.get(nodeId);
		if (file == null) {
			throw new IllegalArgumentException("Unknown node " + nodeId);
		}
		return file;
	}

	/** The address {@code src} uses to reach {@code dst}. */
	public InetAddress linkAddress(String src, String dst) {
		return link(src, dst).address();
	}

	/** Cuts the connections {@code src} opens to {@code dst}, but not the ones {@code dst} opens to {@code src}. */
	public void blockLink(String src, String dst) {
		link(src, dst).proxies().forEach(LinkProxy::block);
	}

	/** Cuts every link from and to {@code nodeId}. Clients connecting directly to the node are not affected. */
	public void isolate(String nodeId) {
		partition(Set.of(nodeId), difference(nodeIds, Set.of(nodeId)));
	}

	/** Cuts every link between a node of {@code sideA} and a node of {@code sideB}, in both directions. */
	public void partition(Set<String> sideA, Set<String> sideB) {
		for (String a : sideA) {
			if (sideB.contains(a)) {
				throw new IllegalArgumentException("Node " + a + " is on both sides of the partition");
			}
		}
		for (String a : sideA) {
			for (String b : sideB) {
				blockLink(a, b);
				blockLink(b, a);
			}
		}
	}

	/** Restores every link. */
	public void heal() {
		links.values().forEach(state -> state.proxies().forEach(LinkProxy::unblock));
	}

	@Override
	public void close() {
		links.values().forEach(state -> state.proxies().forEach(LinkProxy::close));
	}

	private String hostsFileContent(String self) {
		var content = new StringBuilder();
		content.append("# Hosts file of ").append(hostname(self)).append(", generated by the test Network\n");
		content.append("127.0.0.1 localhost\n");
		machineHostname().ifPresent(name -> content.append("127.0.0.1 ").append(name).append('\n'));
		content.append("127.0.0.1 ").append(hostname(self)).append('\n');
		for (String other : nodeIds) {
			if (!other.equals(self)) {
				var address = linkAddress(self, other).getHostAddress();
				content.append(address).append(' ').append(hostname(other)).append('\n');
			}
		}
		return content.toString();
	}

	/** The JDK resolves {@code InetAddress.getLocalHost()} through the hosts file too, so map the machine name. */
	private static Optional<String> machineHostname() {
		try {
			var name = InetAddress.getLocalHost().getHostName();
			return name.equals("localhost") ? Optional.empty() : Optional.of(name);
		} catch (UnknownHostException e) {
			return Optional.empty();
		}
	}

	private LinkState link(String src, String dst) {
		var state = links.get(new Link(src, dst));
		if (state == null) {
			throw new IllegalArgumentException("No link " + src + " -> " + dst);
		}
		return state;
	}

	private static Set<String> difference(List<String> all, Set<String> removed) {
		var result = new HashSet<>(all);
		result.removeAll(removed);
		return result;
	}
}
