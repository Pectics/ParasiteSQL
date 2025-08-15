package me.pectics.parasitesql;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class Launcher {

    // ========= 停机协调（Pterodactyl stop + SIGTERM） =========
    private static final CountDownLatch QUIT = new CountDownLatch(1);
    private static final AtomicBoolean STOPPING = new AtomicBoolean(false);

    private static void gracefulStop(DB db) {
        if (db == null) return;
        if (STOPPING.compareAndSet(false, true)) {
            System.out.println("[HOOK] stop requested, shutting down MariaDB...");
            try { db.stop(); } catch (Exception ignored) {}
            QUIT.countDown();
        }
    }

    private static void startStdinStopWatcher(DB db) {
        // 仅用于“面板 Stop”这种直接打一个 stop\n 的情况；REPL 自己也会识别关键词
        Thread t = new Thread(() -> {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String cmd = line.trim().toLowerCase(Locale.ROOT);
                    if (isStopCommand(cmd)) {
                        gracefulStop(db);
                        break;
                    }
                }
            } catch (IOException ignored) {
            } finally {
                gracefulStop(db);
            }
        }, "stdin-stop-watcher");
        t.setDaemon(true);
        t.start();
    }

    private static boolean isStopCommand(@NotNull String s) {
        return s.equals("stop") || s.equals("shutdown") || s.equals("quit") || s.equals("exit") || s.equals("end");
    }

    // ========= 配置 =========
    private static final String CFG_FILE = "parasitesql.properties";

    private static final class Cfg {
        int port;                        // 1..65535
        String bind;                     // 0.0.0.0 / 127.0.0.1 / IP
        String remoteRootHost;           // '%' or '10.0.0.%' or ''(禁用)
        String rootPwd;                  // non-empty
        String charset;                  // utf8mb4
        String collation;                // utf8mb4_unicode_ci
        int bufferPoolMb;                // >=32
        Path baseDir;                    // base
        Path dataDir;                    // data
        boolean enableRepl;              // 启动 SQL REPL

        @NotNull Properties toProperties() {
            Properties p = new Properties();
            p.setProperty("port", String.valueOf(port));
            p.setProperty("bind", bind);
            p.setProperty("remoteRootHost", remoteRootHost);
            p.setProperty("rootPwd", rootPwd);
            p.setProperty("charset", charset);
            p.setProperty("collation", collation);
            p.setProperty("bufferPoolMb", String.valueOf(bufferPoolMb));
            p.setProperty("baseDir", baseDir.toString());
            p.setProperty("dataDir", dataDir.toString());
            p.setProperty("repl", String.valueOf(enableRepl));
            return p;
        }

        static @NotNull Cfg from(@NotNull Properties fileProps) throws IOException {
            Cfg c = new Cfg();
            // 默认值
            int dPort = getInt(System.getProperty("db.port"), getInt(fileProps.getProperty("port"), 3306));
            String dBind = or(System.getProperty("db.bind"), fileProps.getProperty("bind"), "0.0.0.0");
            String dRemote = or(System.getProperty("db.remoteRootHost"), fileProps.getProperty("remoteRootHost"), "%"); // '%' 表示允许任意远程
            String dPwd = or(System.getProperty("db.root"), fileProps.getProperty("rootPwd"),
                    UUID.randomUUID().toString().replace("-", "").substring(0, 8));
            String dCharset = or(System.getProperty("db.charset"), fileProps.getProperty("charset"), "utf8mb4");
            String dColl = or(System.getProperty("db.collation"), fileProps.getProperty("collation"), "utf8mb4_unicode_ci");
            int dBPMb = getInt(System.getProperty("db.bufferPoolMb"), getInt(fileProps.getProperty("bufferPoolMb"), 128));
            Path dBase = Paths.get(or(System.getProperty("db.base"), fileProps.getProperty("baseDir"), "./mariadb_base"));
            Path dData = Paths.get(or(System.getProperty("db.data"), fileProps.getProperty("dataDir"), "./mariadb_data"));
            boolean dRepl = getBool(System.getProperty("db.repl"), getBool(fileProps.getProperty("repl"), true));

            // 交互式确认/修改
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            System.out.println("=== ParasiteSQL Interactive Setup ===  (ENTER 使用默认)");
            c.port = askInt(br, "DB port", dPort, 1, 65535);
            c.bind = askStr(br, "Bind address (0.0.0.0/127.0.0.1/IP)", dBind, s -> !s.isBlank());

            // 远程 root：留空 = 禁用，'%' = 任意远程，或 10.0.0.% 这种网段
            String hostHint = dRemote.isBlank() ? "(empty to DISABLE)" : dRemote;
            c.remoteRootHost = askStr(br, "Remote root host ('%', '10.0.0.%' or empty to disable)", hostHint, s -> true).trim();
            if (c.remoteRootHost.equalsIgnoreCase("(empty to DISABLE)")) c.remoteRootHost = dRemote; // 处理 hint 回车

            c.rootPwd = askPassword(dPwd);
            c.charset = askStr(br, "Character set", dCharset, s -> !s.isBlank());
            c.collation = askStr(br, "Collation", dColl, s -> !s.isBlank());
            c.bufferPoolMb = askInt(br, "InnoDB buffer pool (MB)", dBPMb, 32, 1_048_576);
            c.baseDir = askPath(br, "Base dir", dBase);
            c.dataDir = askPath(br, "Data dir", dData);
            c.enableRepl = askBool(br, dRepl);

            Files.createDirectories(c.baseDir);
            Files.createDirectories(c.dataDir);

            System.out.println("=== Config Summary ===");
            System.out.printf(Locale.ROOT, "Port=%d, Bind=%s, RemoteRootHost=%s%n", c.port, c.bind, c.remoteRootHost.isBlank() ? "<disabled>" : c.remoteRootHost);
            System.out.printf(Locale.ROOT, "Charset=%s, Collation=%s, BufferPool=%dMB%n", c.charset, c.collation, c.bufferPoolMb);
            System.out.printf(Locale.ROOT, "BaseDir=%s%nDataDir=%s%nREPL=%s%n", c.baseDir.toAbsolutePath(), c.dataDir.toAbsolutePath(), c.enableRepl);

            return c;
        }

        @Contract(pure = true)
        private static @NotNull String or(String @NotNull ... v) {
            for (String s : v) if (s != null && !s.isBlank()) return s;
            return "";
        }

        private static int getInt(String v, int def) {
            if (v == null || v.isBlank()) return def;
            try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
        }

        private static boolean getBool(String v, boolean def) {
            if (v == null || v.isBlank()) return def;
            String s = v.trim().toLowerCase(Locale.ROOT);
            return s.equals("1") || s.equals("true") || s.equals("yes") || s.equals("y");
        }

        private static int askInt(@NotNull BufferedReader br, String title, int def, int min, int max) throws IOException {
            while (true) {
                System.out.printf(Locale.ROOT, "%s [%d]:\n", title, def);
                String s = br.readLine();
                if (s == null || s.isBlank()) return def;
                try {
                    int x = Integer.parseInt(s.trim());
                    if (x >= min && x <= max) return x;
                } catch (NumberFormatException ignore) {}
                System.out.println("Invalid number.");
            }
        }

        private static String askStr(@NotNull BufferedReader br, String title, String def, java.util.function.Predicate<String> ok) throws IOException {
            while (true) {
                System.out.printf("%s [%s]:\n", title, def);
                String s = br.readLine();
                if (s == null || s.isBlank()) return def;
                s = s.trim();
                if (ok.test(s)) return s;
                System.out.println("Invalid input.");
            }
        }

        private static boolean askBool(@NotNull BufferedReader br, boolean def) throws IOException {
            while (true) {
                System.out.printf("%s [%s] (y/n):\n", "Enable SQL REPL", def ? "Y" : "N");
                String s = br.readLine();
                if (s == null || s.isBlank()) return def;
                s = s.trim().toLowerCase(Locale.ROOT);
                if (s.equals("y") || s.equals("yes") || s.equals("true")) return true;
                if (s.equals("n") || s.equals("no") || s.equals("false")) return false;
                System.out.println("Please answer y/n.");
            }
        }

        private static Path askPath(@NotNull BufferedReader br, String title, Path def) throws IOException {
            while (true) {
                System.out.printf("%s [%s]:\n", title, def);
                String s = br.readLine();
                if (s == null || s.isBlank()) return def;
                Path p = Paths.get(s.trim());
                try {
                    Files.createDirectories(p);
                    return p;
                } catch (Exception e) {
                    System.out.println("Cannot create/access path: " + e.getMessage());
                }
            }
        }

        private static String askPassword(String def) throws IOException {
            Console cn = System.console();
            if (cn != null) {
                char[] pw = cn.readPassword("%s [%s]:\n", "Root password", def);
                return (pw == null || pw.length == 0) ? def : new String(pw);
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            System.out.printf("%s [%s]: ", "Root password", def);
            String s = br.readLine();
            return (s == null || s.isBlank()) ? def : s.trim();
        }
    }

    private static @NotNull Properties loadCfgFile(Path file) {
        Properties p = new Properties();
        if (Files.isReadable(file)) {
            try (InputStream in = Files.newInputStream(file)) {
                p.load(in);
            } catch (Exception e) {
                System.err.println("[WARN] Failed to read " + file + ": " + e.getMessage());
            }
        }
        return p;
    }

    private static void saveCfgFile(Path file, Properties p) {
        try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            p.store(out, "ParasiteSQL persisted config");
            System.out.println("[CONF] Saved to " + file.toAbsolutePath());
        } catch (Exception e) {
            System.err.println("[WARN] Failed to save config: " + e.getMessage());
        }
    }

    // ========= 主流程 =========
    public static void main(String[] args) throws Exception {
        Path cfgPath = Paths.get(CFG_FILE);
        Properties fileProps = loadCfgFile(cfgPath);
        Cfg cfg = Cfg.from(fileProps);

        // —— 构建 MariaDB4j 配置 —— //
        DBConfigurationBuilder b = DBConfigurationBuilder.newBuilder();
        b.setPort(cfg.port);
        b.setBaseDir(cfg.baseDir.toFile());
        b.setDataDir(cfg.dataDir.toFile());
        b.setSecurityDisabled(false);

        b.addArg("--bind-address=" + cfg.bind);
        b.addArg("--skip-name-resolve");
        b.addArg("--character-set-server=" + cfg.charset);
        b.addArg("--collation-server=" + cfg.collation);
        b.addArg("--innodb-buffer-pool-size=" + cfg.bufferPoolMb + "M");
        b.addArg("--innodb-file-per-table=1");

        // —— 生成 init-file：仅配置 root（localhost + 远程可选） —— //
        String rootPwdEsc = cfg.rootPwd.replace("'", "''");
        StringBuilder init = new StringBuilder();
        init.append("-- Idempotent root setup; NO extra DB/users\n");
        init.append("CREATE USER IF NOT EXISTS 'root'@'localhost' IDENTIFIED BY '").append(rootPwdEsc).append("';\n");
        init.append("ALTER USER 'root'@'localhost' IDENTIFIED BY '").append(rootPwdEsc).append("';\n");
        init.append("GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;\n");
        if (!cfg.remoteRootHost.isBlank()) {
            init.append("CREATE USER IF NOT EXISTS 'root'@'").append(cfg.remoteRootHost).append("' IDENTIFIED BY '").append(rootPwdEsc).append("';\n");
            init.append("ALTER USER 'root'@'").append(cfg.remoteRootHost).append("' IDENTIFIED BY '").append(rootPwdEsc).append("';\n");
            init.append("GRANT ALL PRIVILEGES ON *.* TO 'root'@'").append(cfg.remoteRootHost).append("' WITH GRANT OPTION;\n");
        } else {
            // 若之前存在远程 root，则锁定
            init.append("ALTER USER IF EXISTS 'root'@'%' ACCOUNT LOCK;\n");
        }
        init.append("FLUSH PRIVILEGES;\n");

        Path initFile = cfg.baseDir.resolve("init_root.sql").toAbsolutePath();
        Files.createDirectories(initFile.getParent());
        Files.writeString(initFile, init.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        b.addArg("--init-file=" + initFile);

        // —— 启动 MariaDB —— //
        DB db = DB.newEmbeddedDB(b.build());
        db.start();

        // —— 保存配置文件（成功启动后再持久化） —— //
        saveCfgFile(cfgPath, cfg.toProperties());

        // —— 探活 —— //
        String jdbcUrl = "jdbc:mariadb://127.0.0.1:" + cfg.port + "/?useUnicode=true&characterEncoding=" + cfg.charset;
        try (Connection c = DriverManager.getConnection(jdbcUrl, "root", cfg.rootPwd);
             Statement s = c.createStatement()) {
            s.execute("SELECT 1");
            System.out.printf(Locale.ROOT, "[OK] MariaDB up on %s:%d; root configured.%n", cfg.bind, cfg.port);
        } catch (Exception e) {
            System.err.println("[WARN] Local root probe failed: " + e.getMessage());
        }

        // —— 停机钩子 —— //
        startStdinStopWatcher(db);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> gracefulStop(db)));

        System.out.println("[READY] MariaDB listening. Type 'stop' here or use panel Stop to shutdown.");
        QUIT.await();

        System.out.println("[EXIT] MariaDB stopped. Bye.");
    }

}
