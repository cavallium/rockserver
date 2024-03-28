#!/bin/bash -e
thrift -r --gen java:generated_annotations=suppress -out src/main/java src/main/resources/it/cavallium/rockserver/core/resources/rocksdb.thrift
