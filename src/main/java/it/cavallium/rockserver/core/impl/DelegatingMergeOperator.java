package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegatingMergeOperator extends FFMAbstractMergeOperator {

	private static final Logger LOG = LoggerFactory.getLogger(DelegatingMergeOperator.class);
	private volatile FFMAbstractMergeOperator delegate;

	public DelegatingMergeOperator(String name, FFMAbstractMergeOperator delegate) {
		super(name);
		this.delegate = Objects.requireNonNull(delegate);
	}

	public void setDelegate(FFMAbstractMergeOperator newDelegate) {
		FFMAbstractMergeOperator old = this.delegate;
		this.delegate = Objects.requireNonNull(newDelegate);
		if (old != null && old != newDelegate) {
			try {
				old.close();
			} catch (Exception e) {
				LOG.warn("Failed to close old delegate merge operator", e);
			}
		}
	}

	public FFMAbstractMergeOperator getDelegate() {
		return delegate;
	}

	@Override
	public Buf merge(Buf key, Buf existingValue, List<Buf> operands) {
		return delegate.merge(key, existingValue, operands);
	}

	@Override
	public Buf partialMergeMulti(Buf key, List<Buf> operands) {
		return delegate.partialMergeMulti(key, operands);
	}

	@Override
	@Deprecated
	public Buf partialMerge(Buf key, Buf leftOperand, Buf rightOperand) {
		return delegate.partialMerge(key, leftOperand, rightOperand);
	}

	@Override
	public void close() {
		try {
			if (delegate != null) {
				delegate.close();
			}
		} catch (Exception e) {
			LOG.warn("Failed to close delegate merge operator", e);
		} finally {
			super.close();
		}
	}
}
