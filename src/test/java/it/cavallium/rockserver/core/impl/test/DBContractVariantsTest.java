package it.cavallium.rockserver.core.impl.test;

/**
 * Discoverable entry points for the complete {@link DBTest} contract matrix.
 *
 * <p>The original variants live as nested classes in {@link DBTestImpl}. Maven Surefire excludes
 * nested classes by default, so none of their inherited tests ran in the normal build. These
 * top-level adapters keep the existing schema fixtures while making every variant part of CI.</p>
 */
final class DBFixedAndVarKeysWithoutValueContractTest
		extends DBTestImpl.DBFixedAndVarKeysWithoutValueTest { }

final class DBFixedAndVarKeysWithValueContractTest
		extends DBTestImpl.DBFixedAndVarKeysWithValueTest { }

final class DBFixedWithoutValueContractTest
		extends DBTestImpl.DBFixedWithoutValueTest { }

final class DBFixedWithValueContractTest
		extends DBTestImpl.DBFixedWithValueTest { }

final class DBVarKeysWithoutValueContractTest
		extends DBTestImpl.DBVarKeysWithoutValueTest { }

final class DBVarKeysWithValueContractTest
		extends DBTestImpl.DBVarKeysWithValueTest { }
