package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.extension.AnnotatedElementContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDirFactory;

/** Durability fixtures must reside on persistent storage rather than host /tmp. */
public final class RatisDiskTestDirectory implements TempDirFactory {
    @Override public Path createTempDirectory(AnnotatedElementContext element, ExtensionContext context) throws Exception {
        Path parent = Path.of("target", "ratis-disk-tests").toAbsolutePath();
        Files.createDirectories(parent);
        String type = Files.getFileStore(parent).type();
        if (type.equals("tmpfs") || type.equals("ramfs") || type.equals("devtmpfs")) {
            throw new IllegalStateException("Ratis durability fixtures require persistent storage: " + type);
        }
        return Files.createTempDirectory(parent, "ratis-");
    }
}
