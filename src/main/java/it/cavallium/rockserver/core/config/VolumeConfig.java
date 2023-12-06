package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;

import java.nio.file.Path;

public interface VolumeConfig {

	Path volumePath() throws GestaltException;

	long targetSizeBytes() throws GestaltException;
}
