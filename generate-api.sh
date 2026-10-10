#!/bin/bash -e
thrift -r --gen java:generated_annotations=suppress -out src/main/java src/main/resources/it/cavallium/rockserver/core/resources/rocksdb.thrift
thrift --gen java:generated_annotations=suppress -out src/main/java src/main/resources/it/cavallium/rockserver/core/resources/exports.thrift
sed -i -e 's/[[:blank:]]*$//' -e ':a;/^\n*$/{$d;N;ba;}' src/main/java/it/cavallium/rockserver/core/common/api/Export*.java src/main/java/it/cavallium/rockserver/core/common/api/RocksDBExports.java
thrift --gen java:generated_annotations=suppress -out src/main/java src/main/resources/it/cavallium/rockserver/core/resources/cdc.thrift
python3 - <<'NORMALIZE'
from pathlib import Path
root=Path('src/main/java/it/cavallium/rockserver/core/common/api')
paths=[root/'Capabilities.java',root/'CdcMode.java',*root.glob('cdc/*.java')]
for path in paths:
    path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines()).rstrip()+'\n')
NORMALIZE
