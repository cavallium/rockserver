package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toByteArray;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.cavallium.buffer.Buf;
import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Conversions between the protobuf messages and the core types. */
final class GrpcProtoMapping {

	private GrpcProtoMapping() {
	}

	static KV unmapKVHeap(it.cavallium.rockserver.core.common.KV kv) {
		if (kv == null) return null;
		var result = KV.newBuilder();
		for (@NotNull Buf key : kv.keys().keys()) {
			result.addKeys(UnsafeByteOperations.unsafeWrap(toByteArray(key)));
		}
		return result
				.setValue(unmapValueHeap(kv.value()))
				.build();
	}

	static ByteString unmapValueHeap(@Nullable Buf value) {
		if (value == null) return null;
		return Utils.toByteString(value);
	}

	static ColumnSchema mapColumnSchema(it.cavallium.rockserver.core.common.api.proto.ColumnSchema schema) {
		return ColumnSchema.of(mapKeysLength(schema.getFixedKeysCount(), schema::getFixedKeys),
				mapVariableTailKeys(schema.getVariableTailKeysCount(), schema::getVariableTailKeys),
				schema.getHasValue(),
				schema.hasMergeOperatorName() ? schema.getMergeOperatorName() : null,
				schema.hasMergeOperatorVersion() ? schema.getMergeOperatorVersion() : null,
				schema.hasMergeOperatorClass() ? schema.getMergeOperatorClass() : null
		);
	}

	static it.cavallium.rockserver.core.common.api.proto.ColumnSchema unmapColumnSchema(@NotNull ColumnSchema schema) {
		var builder = it.cavallium.rockserver.core.common.api.proto.ColumnSchema.newBuilder()
				.addAllFixedKeys(unmapFixedKeys(schema))
				.addAllVariableTailKeys(unmapVariableTailKeys(schema))
				.setHasValue(schema.hasValue());
		if (schema.mergeOperatorName() != null) {
			builder.setMergeOperatorName(schema.mergeOperatorName());
		}
		if (schema.mergeOperatorVersion() != null) {
			builder.setMergeOperatorVersion(schema.mergeOperatorVersion());
		}
		if (schema.mergeOperatorClass() != null) {
			builder.setMergeOperatorClass(schema.mergeOperatorClass());
		}
		return builder.build();
	}

	static Iterable<Integer> unmapFixedKeys(@NotNull ColumnSchema schema) {
		var result = new IntArrayList(schema.fixedLengthKeysCount());
		for (int i = 0; i < schema.fixedLengthKeysCount(); i++) {
			result.add(schema.key(i));
		}
		return result;
	}

	static Iterable<it.cavallium.rockserver.core.common.api.proto.ColumnHashType> unmapVariableTailKeys(@NotNull ColumnSchema schema) {
		var result = new ArrayList<it.cavallium.rockserver.core.common.api.proto.ColumnHashType>(schema.variableTailKeys().size());
		for (it.cavallium.rockserver.core.common.ColumnHashType variableTailKey : schema.variableTailKeys()) {
			result.add(switch (variableTailKey) {
				case XXHASH32 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.XXHASH32;
				case XXHASH8 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.XXHASH8;
				case ALLSAME8 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.ALLSAME8;
				case FIXEDINTEGER32 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.FIXEDINTEGER32;
			});
		}
		return result;
	}

	static IntList mapKeysLength(int count, Int2IntFunction keyGetterAt) {
		var l = new IntArrayList(count);
		for (int i = 0; i < count; i++) {
			l.add((int) keyGetterAt.apply(i));
		}
		return l;
	}

	static ObjectList<ColumnHashType> mapVariableTailKeys(int count,
			Int2ObjectFunction<it.cavallium.rockserver.core.common.api.proto.ColumnHashType> variableTailKeyGetterAt) {
		var l = new ObjectArrayList<ColumnHashType>(count);
		for (int i = 0; i < count; i++) {
			l.add(switch (variableTailKeyGetterAt.apply(i)) {
				case XXHASH32 -> ColumnHashType.XXHASH32;
				case XXHASH8 -> ColumnHashType.XXHASH8;
				case ALLSAME8 -> ColumnHashType.ALLSAME8;
				case FIXEDINTEGER32 -> ColumnHashType.FIXEDINTEGER32;
				case UNRECOGNIZED -> throw new UnsupportedOperationException();
			});
		}
		return l;
	}

	static Keys mapKeys(List<ByteString> wireKeys) {
		var segments = new Buf[wireKeys.size()];
		for (int i = 0; i < segments.length; i++) {
			segments[i] = toBuf(wireKeys.get(i));
		}
		return new Keys(segments);
	}

	static KVBatch mapKVBatch(List<KV> entries) {
		var keys = new ArrayList<Keys>(entries.size());
		var values = new ArrayList<Buf>(entries.size());
		for (var entry : entries) {
			keys.add(mapKeys(entry.getKeysList()));
			values.add(toBuf(entry.getValue()));
		}
		return new KVBatchRef(keys, values);
	}
}
