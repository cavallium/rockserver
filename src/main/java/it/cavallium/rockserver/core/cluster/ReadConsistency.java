package it.cavallium.rockserver.core.cluster;

public enum ReadConsistency {
	LINEARIZABLE(false),
	BOUNDED_STALENESS(true),
	FOLLOWER(true),
	LOCAL(true);

	private final boolean followerAllowed;

	ReadConsistency(boolean followerAllowed) {
		this.followerAllowed = followerAllowed;
	}

	public boolean followerAllowed() {
		return followerAllowed;
	}
}
