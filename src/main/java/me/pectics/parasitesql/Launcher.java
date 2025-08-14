package me.pectics.parasitesql;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * ParasiteSQL: run MariaDB inside a Pterodactyl MC container via MariaDB4j.
 * Requirements from user:
 * 1) Only ONE public port; read it from -Ddb.port, else prompt at startup.
 * 2) Provide root remote access with default password qwerty123456.
 * 3) Do NOT create any extra users or databases other than root.
 */
public class Launcher {

    // Graceful stop coordination for Pterodactyl "stop" (stdin) + SIGTERM
    private static final java.util.concurrent.CountDownLatch QUIT = new java.util.concurrent.CountDownLatch(1);
    private static final java.util.concurrent.atomic.AtomicBoolean STOPPING = new java.util.concurrent.atomic.AtomicBoolean(false);

    private static void gracefulStop(DB db) {
        if (db == null) return;
        if (STOPPING.compareAndSet(false, true)) {
            System.out.println("[HOOK] stop requested, shutting down MariaDB...");
            try { db.stop(); } catch (Exception ignored) {}
            QUIT.countDown();
        }
    }

    private static void startStdinWatcher(DB db) {
        Thread t = new Thread(() -> {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String cmd = line.trim().toLowerCase();
                    if (cmd.equals("stop") || cmd.equals("shutdown") || cmd.equals("end") || cmd.equals("quit") || cmd.equals("exit")) {
                        gracefulStop(db);
                        break;
                    }
                }
            } catch (IOException ignored) {
            } finally {
                // stdin closed unexpectedly -> also stop
                gracefulStop(db);
            }
        }, "stdin-stop-watcher");
        t.setDaemon(true);
        t.start();
    }

    private static int readPortInteractive() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("Enter DB port (e.g. 3306): ");
            String line = br.readLine();
            if (line == null) continue; // keep waiting if stdin not ready
            line = line.trim();
            try {
                int p = Integer.parseInt(line);
                if (p > 0 && p < 65536) return p;
            } catch (NumberFormatException ignored) {}
            System.out.println("Invalid port. Try again.");
        }
    }

    public static void main(String[] args) throws Exception {
        // 1) Resolve port from -Ddb.port or interactive prompt
        int port;
        String prop = System.getProperty("db.port");
        if (prop != null && !prop.isBlank()) {
            port = Integer.parseInt(prop.trim());
        } else {
            port = readPortInteractive();
        }

        // 2) Prepare dirs under Pterodactyl persistent work dir
        Path baseDir = Paths.get("./mariadb_base");
        Path dataDir = Paths.get("./mariadb_data");
        Files.createDirectories(baseDir);
        Files.createDirectories(dataDir);

        // 3) Build MariaDB4j config (no skip-grant-tables; bind to 0.0.0.0)
        DBConfigurationBuilder cfg = DBConfigurationBuilder.newBuilder();
        cfg.setPort(port);
        cfg.setBaseDir(baseDir.toFile());
        cfg.setDataDir(dataDir.toFile());
        cfg.setSecurityDisabled(false); // critical: enable grants
        cfg.addArg("--bind-address=0.0.0.0");
        cfg.addArg("--character-set-server=utf8mb4");
        cfg.addArg("--collation-server=utf8mb4_unicode_ci");
        // keep memory modest; adjust if you have more
        cfg.addArg("--innodb-buffer-pool-size=128M");

        // 4) Create idempotent init SQL to set root password & remote root. No extra DB/users.
        String rootPwd = System.getProperty("db.root", "qwerty123456");
        Path initFile = Paths.get("./init.sql");
        String initSql = String.join("\n",
                "-- Idempotent root setup; NO extra DB or users",
                "ALTER USER 'root'@'localhost' IDENTIFIED BY '" + rootPwd + "';",
                "CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '" + rootPwd + "';",
                "GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;",
                "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;",
                "FLUSH PRIVILEGES;"
        );
        Files.writeString(initFile, initSql);
        cfg.addArg("--init-file=" + initFile.toAbsolutePath());

        // 5) Start DB (will download binaries on first run if internet is available)
        DB db = DB.newEmbeddedDB(cfg.build());
        db.start();

        // 6) Quick probe using configured root credentials (connect locally)
        try (Connection c = DriverManager.getConnection(
                "jdbc:mariadb://127.0.0.1:" + port + "/", "root", rootPwd);
             Statement s = c.createStatement()) {
            s.execute("SELECT 1");
            System.out.println("[OK] MariaDB is up on 0.0.0.0:" + port + ", root password set.");
        } catch (Exception e) {
            System.err.println("[WARN] Local root probe failed: " + e.getMessage());
        }

        // 7) Hook panel stop via stdin + add shutdown hook for SIGTERM; then wait
        startStdinWatcher(db);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> gracefulStop(db)));
        System.out.println("[READY] MariaDB listening. Type 'stop' here or use panel Stop to shutdown.");
        QUIT.await();
        System.out.println("[EXIT] MariaDB stopped. Goodbye.");
    }

}