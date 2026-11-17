package io.clickstream;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Loads modular Flink SQL files from filesystem or classpath, substitutes
 * environment placeholders, executes DDL/VIEW statements, and bundles
 * INSERT statements into a unified StatementSet.
 */
public final class SqlScriptUtils {

    private SqlScriptUtils() {
    }

    public static String loadSqlResourceOrFile(String pathOrResource, Map<String, String> vars) throws IOException {
        String raw;
        Path fsPath = Path.of(pathOrResource);
        if (Files.exists(fsPath)) {
            raw = Files.readString(fsPath, StandardCharsets.UTF_8);
        } else {
            String normalized = pathOrResource.startsWith("/") ? pathOrResource : "/" + pathOrResource;
            try (InputStream in = SqlScriptUtils.class.getResourceAsStream(normalized)) {
                if (in == null) {
                    throw new IOException("SQL script not found on filesystem or classpath: " + pathOrResource);
                }
                raw = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        return substituteVariables(raw, vars);
    }

    public static String substituteVariables(String sql, Map<String, String> vars) {
        String resolved = sql;
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            resolved = resolved.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return resolved;
    }

    public static List<String> splitStatements(String sqlScript) {
        StringBuilder cleaned = new StringBuilder();
        for (String line : sqlScript.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("--") || trimmed.isEmpty()) {
                continue;
            }
            cleaned.append(line).append('\n');
        }
        List<String> statements = new ArrayList<>();
        for (String part : cleaned.toString().split(";")) {
            String stmt = part.trim();
            if (!stmt.isEmpty()) {
                statements.add(stmt);
            }
        }
        return statements;
    }

    public static void executeDdlScript(TableEnvironment tEnv, String sqlScript) {
        for (String stmt : splitStatements(sqlScript)) {
            tEnv.executeSql(stmt);
        }
    }

    public static TableResult executeInsertScriptAsStatementSet(TableEnvironment tEnv, String insertScript) {
        StatementSet statementSet = tEnv.createStatementSet();
        int count = 0;
        for (String stmt : splitStatements(insertScript)) {
            statementSet.addInsertSql(stmt);
            count++;
        }
        if (count == 0) {
            throw new IllegalArgumentException("No INSERT statements found in script");
        }
        return statementSet.execute();
    }
}
