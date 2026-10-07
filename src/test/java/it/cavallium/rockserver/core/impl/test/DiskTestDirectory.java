package it.cavallium.rockserver.core.impl.test;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.extension.AnnotatedElementContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDirFactory;

/** Keep file-durability fixtures off the host's volatile /tmp filesystem. */
public final class DiskTestDirectory implements TempDirFactory {
    @Override public Path createTempDirectory(AnnotatedElementContext element, ExtensionContext context) throws Exception {
        Path parent = Path.of("target", "native-disk-tests").toAbsolutePath();
        Files.createDirectories(parent);
        String type = Files.getFileStore(parent).type();
        if (type.equals("tmpfs") || type.equals("ramfs") || type.equals("devtmpfs")) {
            throw new IllegalStateException("Native durability fixtures require a persistent filesystem: " + type);
        }
        return Files.createTempDirectory(parent, "native-");
    }
}
