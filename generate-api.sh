#!/bin/bash -e
thrift -r --gen java:generated_annotations=suppress -out src/main/java src/main/resources/it/cavallium/rockserver/core/resources/rocksdb.thrift
thrift --gen java:generated_annotations=suppress -out src/main/java src/main/resources/it/cavallium/rockserver/core/resources/exports.thrift
sed -i -e 's/[[:blank:]]*$//' -e ':a;/^\n*$/{$d;N;ba;}' src/main/java/it/cavallium/rockserver/core/common/api/Export*.java src/main/java/it/cavallium/rockserver/core/common/api/RocksDBExports.java
