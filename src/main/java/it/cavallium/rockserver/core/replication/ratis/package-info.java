/**
 * Rockserver-owned production boundary for Apache Ratis integration.
 *
 * <p>Classes in this package must not import {@code org.apache.ratis.*}. The
 * concrete Ratis implementation lives in a classpath-isolated adapter or an
 * external process until the selected Ratis release is safe for Rockserver's
 * production JPMS module path, or until a shaded/relocated adapter artifact is
 * proven by the packaging compatibility gates.
 */
package it.cavallium.rockserver.core.replication.ratis;
