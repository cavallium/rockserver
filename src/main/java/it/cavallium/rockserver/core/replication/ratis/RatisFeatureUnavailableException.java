package it.cavallium.rockserver.core.replication.ratis;

public final class RatisFeatureUnavailableException extends RuntimeException {

	public RatisFeatureUnavailableException(String message) {
		super(message);
	}
}
