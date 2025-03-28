package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBufSimple;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class DBTestImpl {

	public static class DBFixedAndVarKeysWithoutValueTest extends DBTest {

		@Override
		protected Buf getValue1() {
			return emptyBuf();
		}

		@Override
		protected Buf getValue2() {
			return emptyBuf();
		}

		@Override
		protected Buf getValueI(int i) {
			return emptyBuf();
		}

		@Override
		protected boolean getHasValues() {
			return false;
		}

		@Override
		protected Buf getBigValue() {
			return emptyBuf();
		}
	}

	public static class DBFixedAndVarKeysWithValueTest extends DBTest {

	}

	public static class DBFixedWithoutValueTest extends DBTest {

		@Override
		protected boolean getHasValues() {
			return false;
		}

		@Override
		protected Keys getKeyI(int i) {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(4, 7),
					toBufSimple(i)
			});
		}

		@Override
		protected Keys getNotFoundKeyI(int i) {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(5, 6),
					toBufSimple(i)
			});
		}

		@Override
		protected Keys getKey1() {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(4, 6),
					toBufSimple(3)
			});
		}

		@Override
		protected Keys getKey2() {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(4, 6),
					toBufSimple(4)
			});
		}

		@Override
		protected Keys getCollidingKey1() {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(4, 6),
					toBufSimple(5)
			});
		}

		@Override
		protected ObjectList<ColumnHashType> getSchemaVarKeys() {
			return ObjectList.of();
		}

		@Override
		protected Buf getValue1() {
			return emptyBuf();
		}

		@Override
		protected Buf getValue2() {
			return emptyBuf();
		}

		@Override
		protected Buf getValueI(int i) {
			return emptyBuf();
		}

		@Override
		protected Buf getBigValue() {
			return emptyBuf();
		}
	}


	public static class DBFixedWithValueTest extends DBTest {

		@Override
		protected Keys getKeyI(int i) {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(4, 7),
					toBufSimple(i)
			});
		}

		@Override
		protected Keys getNotFoundKeyI(int i) {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(5, 6),
					toBufSimple(i)
			});
		}

		@Override
		protected Keys getKey1() {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(4, 6),
					toBufSimple(3)
			});
		}

		@Override
		protected Keys getKey2() {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(4, 6),
					toBufSimple(4)
			});
		}

		@Override
		protected Keys getCollidingKey1() {
			return new Keys(new Buf[] {
					toBufSimple(3),
					toBufSimple(4, 6),
					toBufSimple(5)
			});
		}

		@Override
		protected ObjectList<ColumnHashType> getSchemaVarKeys() {
			return ObjectList.of();
		}

	}

	public static class DBVarKeysWithoutValueTest extends DBTest {

		protected Keys getKeyI(int i) {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(8, 2, 5, 1, 7, i)
			});
		}

		protected Keys getNotFoundKeyI(int i) {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(8, 2, 5, 1, 0, i)
			});
		}

		protected Keys getKey2() {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(6, 7, 7)
			});
		}

		protected Keys getCollidingKey1() {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(6, 7, -48)
			});
		}

		protected Keys getKey1() {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(6, 7, 8)
			});
		}

		@Override
		protected IntList getSchemaFixedKeys() {
			return IntList.of();
		}

		@Override
		protected Buf getValue1() {
			return emptyBuf();
		}

		@Override
		protected Buf getValue2() {
			return emptyBuf();
		}

		@Override
		protected Buf getValueI(int i) {
			return emptyBuf();
		}

		@Override
		protected boolean getHasValues() {
			return false;
		}

		@Override
		protected Buf getBigValue() {
			return emptyBuf();
		}
	}

	public static class DBVarKeysWithValueTest extends DBTest {

		protected Keys getKeyI(int i) {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(8, 2, 5, 1, 7, i)
			});
		}

		protected Keys getNotFoundKeyI(int i) {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(8, 2, 5, 1, 0, i)
			});
		}

		protected Keys getKey2() {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(6, 7, 7)
			});
		}

		protected Keys getCollidingKey1() {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(6, 7, -48)
			});
		}

		protected Keys getKey1() {
			return new Keys(new Buf[] {
					toBufSimple(1, 2, 3),
					toBufSimple(6, 7, 8)
			});
		}

		@Override
		protected IntList getSchemaFixedKeys() {
			return IntList.of();
		}
	}

}
