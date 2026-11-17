package com.minisqs.tools;

import java.util.HashMap;
import java.util.Map;

/** Parses "--key value" command-line options. */
public final class Args {
    private final Map<String, String> values = new HashMap<>();

    public Args(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) throw new IllegalArgumentException("unexpected argument: " + args[i]);
            String key = args[i].substring(2);
            String value = (i + 1 < args.length && !args[i + 1].startsWith("--")) ? args[++i] : "true";
            values.put(key, value);
        }
    }

    public String str(String key, String def) {
        return values.getOrDefault(key, def);
    }

    public int integer(String key, int def) {
        return values.containsKey(key) ? Integer.parseInt(values.get(key)) : def;
    }

    public double dbl(String key, double def) {
        return values.containsKey(key) ? Double.parseDouble(values.get(key)) : def;
    }

    public boolean bool(String key, boolean def) {
        return values.containsKey(key) ? Boolean.parseBoolean(values.get(key)) : def;
    }
}
