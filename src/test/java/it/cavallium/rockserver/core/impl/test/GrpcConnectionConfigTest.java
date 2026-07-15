package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.client.GrpcConnection.DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
import static it.cavallium.rockserver.core.client.GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY;
import static it.cavallium.rockserver.core.client.GrpcConnection.MIN_MAX_INBOUND_MESSAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.GrpcConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class GrpcConnectionConfigTest {

	private String previousMaxInboundMessageSize;

	@BeforeEach
	void rememberProperty() {
		previousMaxInboundMessageSize = System.getProperty(MAX_INBOUND_MESSAGE_SIZE_PROPERTY);
	}

	@AfterEach
	void restoreProperty() {
		if (previousMaxInboundMessageSize == null) {
			System.clearProperty(MAX_INBOUND_MESSAGE_SIZE_PROPERTY);
		} else {
			System.setProperty(MAX_INBOUND_MESSAGE_SIZE_PROPERTY, previousMaxInboundMessageSize);
		}
	}

	@Test
	void absentPropertyUsesSixtyFourMiBDefault() {
		System.clearProperty(MAX_INBOUND_MESSAGE_SIZE_PROPERTY);

		assertEquals(64 * 1024 * 1024, DEFAULT_MAX_INBOUND_MESSAGE_SIZE);
		assertEquals(DEFAULT_MAX_INBOUND_MESSAGE_SIZE, GrpcConnection.configuredMaxInboundMessageSize());
	}

	@Test
	void blankPropertyUsesDefault() {
		System.setProperty(MAX_INBOUND_MESSAGE_SIZE_PROPERTY, " \t\n");

		assertEquals(DEFAULT_MAX_INBOUND_MESSAGE_SIZE, GrpcConnection.configuredMaxInboundMessageSize());
	}

	@ParameterizedTest
	@ValueSource(ints = {4 * 1024 * 1024, Integer.MAX_VALUE})
	void acceptsInclusiveBounds(int configuredValue) {
		System.setProperty(MAX_INBOUND_MESSAGE_SIZE_PROPERTY, Integer.toString(configuredValue));

		assertEquals(configuredValue, GrpcConnection.configuredMaxInboundMessageSize());
		assertEquals(4 * 1024 * 1024, MIN_MAX_INBOUND_MESSAGE_SIZE);
	}

	@ParameterizedTest
	@ValueSource(strings = {"0", "-1", "4194303", "not-a-number", "2147483648"})
	void rejectsInvalidValuesWithPropertyContext(String configuredValue) {
		System.setProperty(MAX_INBOUND_MESSAGE_SIZE_PROPERTY, configuredValue);

		var error = assertThrows(IllegalArgumentException.class,
				GrpcConnection::configuredMaxInboundMessageSize);
		assertTrue(error.getMessage().contains(MAX_INBOUND_MESSAGE_SIZE_PROPERTY));
		assertTrue(error.getMessage().contains(configuredValue));
	}
}
