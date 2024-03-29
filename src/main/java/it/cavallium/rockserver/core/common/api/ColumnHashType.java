/**
 * Autogenerated by Thrift Compiler (0.19.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package it.cavallium.rockserver.core.common.api;


public enum ColumnHashType implements org.apache.thrift.TEnum {
  XXHASH32(1),
  XXHASH8(2),
  ALLSAME8(3);

  private final int value;

  private ColumnHashType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  @Override
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static ColumnHashType findByValue(int value) { 
    switch (value) {
      case 1:
        return XXHASH32;
      case 2:
        return XXHASH8;
      case 3:
        return ALLSAME8;
      default:
        return null;
    }
  }
}