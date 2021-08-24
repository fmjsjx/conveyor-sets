package com.github.fmjsjx.conveyor.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigUtil {

    private static final class ConfDirHolder {
        private static final String CONF_DIR = System.getProperty("conf.dir", "conf");
    }

    public static final String confDir() {
        return ConfDirHolder.CONF_DIR;
    }

    public static final File confFile(String filename) {
        return new File(confDir(), filename);
    }

    public static final InputStream openConfFile(String filename) throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(confFile(filename)));
    }

    public static final String fixValue(String pattern, Map<String, String> params) {
        var value = pattern;
        for (var entry : params.entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            value = value.replace(k, v);
        }
        return value;
    }

    public static final Map<String, String> params(int productId) {
        return Map.of("${product}", Integer.toString(productId));
    }

    public static final String fixValue(String pattern, int productId) {
        return fixValue(pattern, params(productId));
    }

    public static final File toFile(String filename) {
        return new File(confDir(), filename);
    }

    public static final <R> R loadConfiguration(File file, Function<InputStream, R> loader)
            throws FileNotFoundException, IOException {
        log.info("[app:init] Loading configuration {}", file);
        try (var in = new BufferedInputStream(new FileInputStream(file))) {
            var cfg = loader.apply(in);
            log.debug("[app:init] Loaded configuration <== {}", cfg);
            return cfg;
        }
    }

    public static final <R> R loadConfiguration(String filename, Function<InputStream, R> loader)
            throws FileNotFoundException, IOException {
        return loadConfiguration(toFile(filename), loader);
    }

    public static final List<Path> searchFiles(String pattern) throws IOException {
        if (pattern.endsWith("/")) {
            throw new IllegalArgumentException("the glob pattern expected files but was directories `" + pattern + "`");
        }
        var index = pattern.lastIndexOf('/');
        var glob = pattern.substring(index + 1);
        Path dir;
        if (pattern.startsWith("/")) {
            dir = Paths.get(pattern.substring(0, index)).normalize();
        } else {
            dir = Paths.get(confDir(), pattern.substring(0, index)).normalize();
        }
        try (var stream = Files.newDirectoryStream(dir, glob)) {
            var list = new ArrayList<Path>();
            stream.forEach(list::add);
            return list;
        }
    }

    private ConfigUtil() {
    }

}
