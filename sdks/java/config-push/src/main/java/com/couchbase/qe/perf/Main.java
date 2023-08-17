
package com.couchbase.qe.perf;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.env.*;
import com.couchbase.client.java.json.*;
import com.couchbase.client.core.error.*;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.core.service.ServiceType;

public class Main {

    public static void main(String[] args) {
        final Params params = new Params(args);
        List<ConfigPushClient> clients = new ArrayList<>();

        final ClusterEnvironment env = ClusterEnvironment.builder()
            .timeoutConfig(timeout -> TimeoutConfig.kvTimeout(Duration.ofMillis(params.timeout)))
            .ioConfig(io -> IoConfig.configPollInterval(Duration.ofMillis(params.configPollInterval)))
            .build();

        try {
            clients = initClients(params, env);
            for (Thread c : clients) {
                c.start();
            }
            TimeUnit.SECONDS.sleep(params.runningTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ConfigPushClient.setStopThread();

        for (Thread c : clients) {
            while (c.isAlive()) {
                try {
                    c.join(100);
                    if (c.isAlive()) {
                        System.err.println(c.getName() + " failed to stop in time");
                    }
                } catch (InterruptedException e) {
                }

            }
        }

        if (env != null) {
            env.shutdown();
        }
    }

    static List<ConfigPushClient> initClients(Params params, ClusterEnvironment env) {
        List<ConfigPushClient> clients = new ArrayList<>(params.threads);
        for (int i = 0; i < params.threads; ++i) {
            ConfigPushClient client = new ConfigPushClient(params, env);
            clients.add(client);
        }
        return clients;
    }
}

final class ConfigPushClient extends Thread {

    Params params;

    private static volatile boolean stopThread = false;

    private final Collection collection;
    private Cluster cluster;

    public ConfigPushClient(Params params, ClusterEnvironment env) {
        this.params = params;
        ClusterOptions options = ClusterOptions.clusterOptions(params.username, params.password);
        options.environment(env);
        this.cluster = Cluster.connect(params.host, options);
        Bucket bucket = cluster.bucket(params.bucket);
        bucket.waitUntilReady(Duration.ofSeconds(100),
                WaitUntilReadyOptions.waitUntilReadyOptions().serviceTypes(ServiceType.KV));
        this.collection = bucket.defaultCollection();
    }

    @Override
    public void run() {

        while (!stopThread) {
            String key = this.params.nextKey();
            try {
                // Get a Document
                GetResult getResult = collection.get(key);
                // Write back
                final JsonObject newContent = this.modifyContent(getResult.contentAsObject());
                try {
                    collection.upsert(key, newContent);
                    printLog("[SUCCESS] Store");
                } catch (CouchbaseException e) {
                    printLog("[FAILURE] Store " + e.getMessage());
                }
            } catch (CouchbaseException e) {
                printLog("[FAILURE] Read " + e.getMessage());
            } catch (Exception e) {
                System.err.println(e);
                break;
            }
        }

        this.cluster.disconnect();
    }

    public static void setStopThread() {
        stopThread = true;
    }

    private JsonObject modifyContent(final JsonObject content) {
        return content;
    }

    private void printLog(String message) {
        LocalDateTime now = LocalDateTime.now();
        System.out.println(now + ", " + message);
    }
}

final class Params {
    // Cluster
    String host;
    String bucket;
    String username;
    String password;

    // Doc
    String scope;
    String coll;

    // Program
    String[] keys;
    int timeout;
    int configPollInterval;
    int threads;
    long items;
    int runningTime;

    // Misc
    final AtomicInteger keyCount = new AtomicInteger();

    public String nextKey() {
        return keys[(keyCount.getAndIncrement() & Integer.MAX_VALUE) % keys.length];
    }

    public Params(String[] args) {
        for (int i = 0; i < args.length; i += 2) {
            String arg = args[i];
            String argValue = (i + 1 < args.length) ? args[i + 1] : "";
            switch (arg) {
                case "--host":
                    this.host = argValue;
                    break;
                case "--bucket":
                    this.bucket = argValue;
                    break;
                case "--username":
                    this.username = argValue;
                    break;
                case "--password":
                    this.password = argValue;
                    break;
                case "--num-items":
                    this.items = Long.parseLong(argValue);
                    break;
                case "--num-threads":
                    this.threads = Integer.parseInt(argValue);
                    break;
                case "--timeout":
                    this.timeout = Integer.parseInt(argValue);
                    break;
                case "--keys":
                    this.keys = argValue.split(",");
                    break;
                case "--time":
                    this.runningTime = Integer.parseInt(argValue);
                    break;
                case "--c-poll-interval":
                    this.configPollInterval = Integer.parseInt(argValue);
                    break;
                default:
                    System.err
                            .println("[WARNING] Unknown option " + arg + ", ignoring value '"
                            + argValue + "'");
            }

        }
        System.err.println("Running benchmark for " + this);
    }

    @Override
    public String toString() {
        return this.runningTime + " sec on host " + this.host + " (" + this.bucket
                + ") with [items: " + this.items + ", threads: " + this.threads + ", timeout: "
                + this.timeout + "us]";
    }

}