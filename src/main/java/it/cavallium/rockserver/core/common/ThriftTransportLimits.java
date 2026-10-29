package it.cavallium.rockserver.core.common;

/** Shared, explicit Thrift transport limits. */
public final class ThriftTransportLimits {

	public static final String CLIENT_MAX_FRAME_SIZE_PROPERTY =
			"it.cavallium.rockserver.thrift.client.max-frame-size-bytes";
	public static final String SERVER_MAX_FRAME_SIZE_PROPERTY =
			"it.cavallium.rockserver.thrift.server.max-frame-size-bytes";

	public static final int DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;
	public static final int MIN_MAX_FRAME_SIZE = 1024 * 1024;

	private ThriftTransportLimits() {}

	public static ClientLimits configuredClientLimits() {
		int maxFrameSize = configuredSize(CLIENT_MAX_FRAME_SIZE_PROPERTY,
				DEFAULT_MAX_FRAME_SIZE,
				MIN_MAX_FRAME_SIZE,
				Integer.MAX_VALUE);
		return new ClientLimits(maxFrameSize);
	}

	public static int configuredServerMaxFrameSize() {
		return configuredSize(SERVER_MAX_FRAME_SIZE_PROPERTY,
				DEFAULT_MAX_FRAME_SIZE,
				MIN_MAX_FRAME_SIZE,
				Integer.MAX_VALUE);
	}

	public static ClientLimits validateClientLimits(int maxFrameSize) {
		validateSize("maxFrameSize",
				maxFrameSize,
				MIN_MAX_FRAME_SIZE,
				Integer.MAX_VALUE);
		return new ClientLimits(maxFrameSize);
	}

	public static int validateServerMaxFrameSize(int maxFrameSize) {
		validateSize("maxFrameSize",
				maxFrameSize,
				MIN_MAX_FRAME_SIZE,
				Integer.MAX_VALUE);
		return maxFrameSize;
	}

	private static int configuredSize(String property, int defaultValue, int minimum, int maximum) {
		String configuredValue = System.getProperty(property);
		if (configuredValue == null || configuredValue.isBlank()) {
			return defaultValue;
		}
		final long parsed;
		try {
			parsed = Long.parseLong(configuredValue.trim());
		} catch (NumberFormatException error) {
			throw new IllegalArgumentException("System property " + property
					+ " must be a base-10 byte count, got: " + configuredValue, error);
		}
		if (parsed < minimum || parsed > maximum) {
			throw new IllegalArgumentException("System property " + property
					+ " must be between " + minimum + " and " + maximum
					+ " bytes, got: " + parsed);
		}
		return (int) parsed;
	}

	private static void validateSize(String name, int value, int minimum, int maximum) {
		if (value < minimum || value > maximum) {
			throw new IllegalArgumentException(name + " must be between " + minimum
					+ " and " + maximum + " bytes, got: " + value);
		}
	}

	public record ClientLimits(int maxFrameSize) {}
}
