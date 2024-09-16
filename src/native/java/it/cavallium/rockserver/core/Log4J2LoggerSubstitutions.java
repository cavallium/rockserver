package it.cavallium.rockserver.core;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import io.netty.util.internal.logging.InternalLoggerFactory;

@TargetClass(value = InternalLoggerFactory.class)
public final class Log4J2LoggerSubstitutions {

	@Substitute
	private static InternalLoggerFactory useLog4J2LoggerFactory(String name) {
		return null;
	}

	@Substitute
	private static InternalLoggerFactory useLog4JLoggerFactory(String name) {
		return null;
	}
}
