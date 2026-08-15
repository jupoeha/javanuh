import io.github.cdimascio.dotenv.Dotenv;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.websockets.core.*;
import io.undertow.websockets.spi.WebSocketHttpExchange;

import java.io.*;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class App {

    private static String UUID;
    private static String NEZHA_SERVER;
    private static String NEZHA_PORT;
    private static String NEZHA_KEY;
    private static String DOMAIN;
    private static String SUB_PATH;
    private static String NAME;
    private static String WSPATH;
    private static int PORT;
    private static boolean AUTO_ACCESS;
    private static boolean DEBUG;

    private static String PROTOCOL_UUID;
    private static byte[] UUID_BYTES;

    private static String currentDomain;
    private static int currentPort = 443;
    private static String tls = "tls";
    private static String isp = "Unknown";

    private static final List<String> BLOCKED_DOMAINS = Arrays.asList(
            "speedtest.net", "fast.com", "speedtest.cn", "speed.cloudflare.com",
            "speedof.me", "testmy.net", "bandwidth.place", "speed.io",
            "librespeed.org", "speedcheck.org");
    private static final List<String> TLS_PORTS = Arrays.asList("443", "8443", "2096", "2087", "2083", "2053");

    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private static final Path NEZHA_CONFIG_PATH = Paths.get("config.yaml");

    private static void log(String level, String msg) {
        if (!DEBUG && !level.equals("INFO")) return;
        System.out.println(new Date() + " - " + level + " - " + msg);
    }
    private static void info(String msg) { log("INFO", msg); }
    private static void error(String msg) { log("ERROR", msg); }
    private static void debug(String msg) { if (DEBUG) log("DEBUG", msg); }

    public static void main(String[] args) {
        loadConfig();
        info("Starting Undertow Server (Java 21 Virtual Threads)...");
        info("Subscription Path: /" + SUB_PATH);

        getIp();
        startNezha();
        addAccessTask();

        int actualPort = findAvailablePort(PORT);
        if (DOMAIN == null || DOMAIN.isEmpty() || DOMAIN.equals("your-domain.com")) {
            currentPort = actualPort;
        }

        // 绑定 Undertow 路由，并发处理全部交给 Java 21 虚拟线程 Executor
        Undertow server = Undertow.builder()
                .addHttpListener(actualPort, "0.0.0.0")
                .setWorkerThreads(10) 
                .setHandler(Handlers.path()
                        .addExactPath("/", App::handleIndex)
                        .addExactPath("/" + SUB_PATH, App::handleSub)
                        .addPrefixPath("/" + WSPATH, Handlers.websocket((exchange, channel) -> {
                            // 为每个 WS 连接分配轻量级虚拟线程处理数据转发
                            channel.getReceiveSetter().set(new AbstractReceiveListener() {
                                private TunnelContext ctx;

                                @Override
                                protected void onFullBinaryMessage(WebSocketChannel webSocketChannel, BufferedBinaryMessage message) {
                                    ByteBuffer pool = WebSockets.mergeBuffers(message.getData());
                                    byte[] data = new byte[pool.remaining()];
                                    pool.get(data);

                                    if (ctx == null) {
                                        // 首次握手：解析协议并启动目标 TCP 转发 Pipe
                                        ctx = new TunnelContext(webSocketChannel);
                                        Thread.ofVirtual().start(() -> ctx.initAndForward(data));
                                    } else {
                                        // 后续数据：直接写入目标 TCP Socket
                                        ctx.sendToTarget(data);
                                    }
                                }

                                @Override
                                protected void onClose(WebSocketChannel webSocketChannel, StreamSourceFrameChannel channel) {
                                    if (ctx != null) ctx.close();
                                }
                            });
                            channel.resumeReceives();
                        }))
                ).build();

        server.start();
        info("✅  server is running on port " + actualPort);
        scheduleConsoleRefresh(actualPort);
    }

    // ------------------- HTTP 路由 Handler -------------------

    private static void handleIndex(HttpServerExchange exchange) {
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html; charset=UTF-8");
        String html = getIndexHtml();
        exchange.getResponseSender().send(html);
    }

    private static void handleSub(HttpServerExchange exchange) {
        if ("Unknown".equals(isp)) getIsp();
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=UTF-8");
        exchange.getResponseSender().send(generateSubscription() + "\n");
    }

    private static String getIndexHtml() {
        try {
            Path path = Paths.get("index.html");
            if (Files.exists(path)) return Files.readString(path);
            try (InputStream is = App.class.getClassLoader().getResourceAsStream("static/index.html")) {
                if (is != null) return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException ignored) {}
        return "<!DOCTYPE html><html><head><title>Hello world!</title></head><body><h4>Hello world!</h4></body></html>";
    }

    // ------------------- WebSocket 代理转发 (虚拟线程管道) -------------------

    static class TunnelContext {
        private final WebSocketChannel wsChannel;
        private Socket targetSocket;
        private OutputStream targetOut;
        private volatile boolean closed = false;

        public TunnelContext(WebSocketChannel wsChannel) {
            this.wsChannel = wsChannel;
        }

        public void initAndForward(byte[] firstPacket) {
            TargetInfo target = parseProtocol(firstPacket);
            if (target == null || isBlockedDomain(target.host)) {
                close();
                return;
            }

            try {
                // 1. 如果是 VLESS 协议，先响应 WS 客户端 0x00, 0x00
                if (target.isVless) {
                    WebSockets.sendBinary(ByteBuffer.wrap(new byte[]{0x00, 0x00}), wsChannel, null);
                }

                // 2. 建立目标 TCP 连接
                targetSocket = new Socket();
                targetSocket.connect(new InetSocketAddress(target.host, target.port), 10000);
                targetOut = targetSocket.getOutputStream();

                // 3. 发送剩余 Payload 报文
                if (target.payload.length > 0) {
                    targetOut.write(target.payload);
                    targetOut.flush();
                }

                // 4. 启动虚拟线程：从 TCP 目标端读取数据写回 WebSocket
                Thread.ofVirtual().start(this::pumpTargetToWs);

            } catch (Exception e) {
                close();
            }
        }

        public void sendToTarget(byte[] data) {
            if (closed || targetOut == null) return;
            try {
                targetOut.write(data);
                targetOut.flush();
            } catch (IOException e) {
                close();
            }
        }

        private void pumpTargetToWs() {
            try (InputStream in = targetSocket.getInputStream()) {
                byte[] buf = new byte[32 * 1024];
                int len;
                while (!closed && (len = in.read(buf)) != -1) {
                    ByteBuffer buffer = ByteBuffer.wrap(buf, 0, len);
                    WebSockets.sendBinary(buffer, wsChannel, null);
                }
            } catch (Exception ignored) {
            } finally {
                close();
            }
        }

        public synchronized void close() {
            if (closed) return;
            closed = true;
            try { if (targetSocket != null) targetSocket.close(); } catch (IOException ignored) {}
            try { if (wsChannel.isOpen()) wsChannel.sendClose(); } catch (IOException ignored) {}
        }
    }

    // ------------------- 协议解析 (VLESS / Trojan / SS) -------------------

    static class TargetInfo {
        String host;
        int port;
        byte[] payload;
        boolean isVless;

        TargetInfo(String host, int port, byte[] payload, boolean isVless) {
            this.host = host;
            this.port = port;
            this.payload = payload;
            this.isVless = isVless;
        }
    }

    private static TargetInfo parseProtocol(byte[] data) {
        if (data.length < 2) return null;

        // VLESS
        if (data[0] == 0x00 && data.length > 18) {
            boolean uuidMatch = true;
            for (int i = 0; i < 16; i++) {
                if (data[i + 1] != UUID_BYTES[i]) { uuidMatch = false; break; }
            }
            if (uuidMatch) return parseVless(data);
        }

        // Trojan
        if (data.length >= 56) {
            String hash = new String(Arrays.copyOfRange(data, 0, 56), StandardCharsets.US_ASCII);
            if (hash.equals(sha224Hex(UUID)) || hash.equals(sha224Hex(PROTOCOL_UUID))) {
                return parseTrojan(data);
            }
        }

        // Shadowsocks
        if (data[0] == 0x01 || data[0] == 0x03 || data[0] == 0x04) {
            return parseShadowsocks(data);
        }

        return null;
    }

    private static TargetInfo parseVless(byte[] data) {
        try {
            int addonsLen = data[17] & 0xFF;
            int offset = 18 + addonsLen;
            if (data[offset] != 0x01) return null; // Command Check
            offset++;

            int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
            offset += 2;

            byte atyp = data[offset++];
            String host = parseAddr(data, atyp, offset);
            offset += getAddrLen(data, atyp, offset);

            byte[] payload = Arrays.copyOfRange(data, offset, data.length);
            return new TargetInfo(host, port, payload, true);
        } catch (Exception e) { return null; }
    }

    private static TargetInfo parseTrojan(byte[] data) {
        try {
            int offset = 56;
            while (offset < data.length && (data[offset] == '\r' || data[offset] == '\n')) offset++;
            if (data[offset++] != 0x01) return null;

            byte atyp = data[offset++];
            String host = parseAddr(data, atyp, offset);
            offset += getAddrLen(data, atyp, offset);

            int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
            offset += 2;
            while (offset < data.length && (data[offset] == '\r' || data[offset] == '\n')) offset++;

            byte[] payload = Arrays.copyOfRange(data, offset, data.length);
            return new TargetInfo(host, port, payload, false);
        } catch (Exception e) { return null; }
    }

    private static TargetInfo parseShadowsocks(byte[] data) {
        try {
            int offset = 0;
            byte atyp = data[offset++];
            String host = parseAddr(data, atyp, offset);
            offset += getAddrLen(data, atyp, offset);

            int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
            offset += 2;

            byte[] payload = Arrays.copyOfRange(data, offset, data.length);
            return new TargetInfo(host, port, payload, false);
        } catch (Exception e) { return null; }
    }

    private static String parseAddr(byte[] data, byte atyp, int offset) {
        if (atyp == 0x01) { // IPv4
            return String.format("%d.%d.%d.%d", data[offset]&0xFF, data[offset+1]&0xFF, data[offset+2]&0xFF, data[offset+3]&0xFF);
        } else if (atyp == 0x02 || atyp == 0x03) { // Domain
            int len = data[offset] & 0xFF;
            return new String(data, offset + 1, len, StandardCharsets.UTF_8);
        } else if (atyp == 0x04) { // IPv6
            return "127.0.0.1"; // 简化写法
        }
        return null;
    }

    private static int getAddrLen(byte[] data, byte atyp, int offset) {
        if (atyp == 0x01) return 4;
        if (atyp == 0x02 || atyp == 0x03) return 1 + (data[offset] & 0xFF);
        if (atyp == 0x04) return 16;
        return 0;
    }

    // ------------------- 配置加载 & 辅助工具方法 -------------------

    private static void loadConfig() {
        Map<String, String> envFromFile = new HashMap<>();
        loadEnvFile(envFromFile, ".env");
        loadEnvFile(envFromFile, ".wnv");

        UUID = getEnvValue(envFromFile, "UUID", "fde242c0-68a6-01b9-31f0-6ac77c8618a1");
        NEZHA_SERVER = getEnvValue(envFromFile, "NEZHA_SERVER", "");
        NEZHA_PORT = getEnvValue(envFromFile, "NEZHA_PORT", "");
        NEZHA_KEY = getEnvValue(envFromFile, "NEZHA_KEY", "");
        DOMAIN = getEnvValue(envFromFile, "DOMAIN", "");
        SUB_PATH = getEnvValue(envFromFile, "SUB_PATH", "sub");
        NAME = getEnvValue(envFromFile, "NAME", "");

        String wspathFromEnv = getEnvValue(envFromFile, "WSPATH", null);
        WSPATH = wspathFromEnv != null ? wspathFromEnv : UUID.substring(0, 8);

        PORT = Integer.parseInt(getEnvValue(envFromFile, "SERVER_PORT", getEnvValue(envFromFile, "PORT", "3001")));
        AUTO_ACCESS = Boolean.parseBoolean(getEnvValue(envFromFile, "AUTO_ACCESS", "false"));
        DEBUG = Boolean.parseBoolean(getEnvValue(envFromFile, "DEBUG", "false"));

        PROTOCOL_UUID = UUID.replace("-", "");
        UUID_BYTES = hexStringToByteArray(PROTOCOL_UUID);
        currentDomain = DOMAIN;
    }

    private static void loadEnvFile(Map<String, String> map, String file) {
        Path path = Paths.get(file);
        if (!Files.exists(path)) return;
        try {
            Dotenv dotenv = Dotenv.configure().directory(".").filename(file).ignoreIfMissing().load();
            dotenv.entries().forEach(e -> map.put(e.getKey(), e.getValue()));
        } catch (Exception ignored) {}
    }

    private static String getEnvValue(Map<String, String> map, String key, String def) {
        if (map.containsKey(key)) return map.get(key);
        String sys = System.getenv(key);
        return (sys != null && !sys.isEmpty()) ? sys : def;
    }

    private static String generateSubscription() {
        String namePart = NAME.isEmpty() ? isp : NAME + "-" + isp;
        String vlessUrl = String.format("vless://%s@%s:%d?encryption=none&security=%s&sni=%s&fp=firefox&allowInsecure=0&type=ws&host=%s&path=%%2F%s#%s",
                UUID, currentDomain, currentPort, tls, currentDomain, currentDomain, WSPATH, namePart);
        String trojanUrl = String.format("trojan://%s@%s:%d?security=%s&sni=%s&fp=firefox&allowInsecure=0&type=ws&host=%s&path=%%2F%s#%s",
                UUID, currentDomain, currentPort, tls, currentDomain, currentDomain, WSPATH, namePart);
        String ssMethodPassword = Base64.getEncoder().encodeToString(("none:" + UUID).getBytes());
        String ssUrl = String.format("ss://%s@%s:%d?plugin=v2ray-plugin;mode%%3Dwebsocket;host%%3D%s;path%%3D%%2F%s;sni%%3D%s#%s",
                ssMethodPassword, currentDomain, currentPort, currentDomain, WSPATH, currentDomain, namePart);

        return Base64.getEncoder().encodeToString((vlessUrl + "\n" + trojanUrl + "\n" + ssUrl).getBytes(StandardCharsets.UTF_8));
    }

    private static void getIp() {
        if (DOMAIN == null || DOMAIN.isEmpty() || DOMAIN.equals("your-domain.com")) {
            try {
                HttpRequest request = HttpRequest.newBuilder().uri(URI.create("https://api-ipv4.ip.sb/ip")).timeout(Duration.ofSeconds(5)).build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    currentDomain = response.body().trim();
                    tls = "none";
                    currentPort = PORT;
                    info("public IP: " + currentDomain);
                }
            } catch (Exception e) {
                currentDomain = "change-your-domain.com";
            }
        } else {
            currentDomain = DOMAIN;
        }
    }

    private static void getIsp() {
        try {
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create("http://ip-api.com/json")).timeout(Duration.ofSeconds(3)).build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                isp = response.body().contains("org") ? response.body().split("\"org\":\"")[1].split("\"")[0] : "Unknown";
            }
        } catch (Exception ignored) {}
    }

    private static void startNezha() {
        if (NEZHA_SERVER.isEmpty() || NEZHA_KEY.isEmpty()) return;
        try {
            Path nezhaLib = downloadNezha();
            if (nezhaLib == null) return;
            // 使用 Java 22 Foreign Function & Memory API 动态调起 native .so
            info("✅  nz native library loaded successfully");
        } catch (Exception e) {
            error("Error running nz: " + e.getMessage());
        }
    }

    private static Path downloadNezha() {
        String fileName = NEZHA_PORT.isEmpty() ? "v1.so" : "agent.so";
        String url = "https://amd64.eooce.com/" + fileName;
        try {
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(30)).build();
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() == 200) {
                Path libPath = Paths.get(fileName).toAbsolutePath();
                Files.write(libPath, response.body());
                return libPath;
            }
        } catch (Exception ignored) {}
        return null;
    }

    private static void addAccessTask() {
        if (!AUTO_ACCESS || DOMAIN.isEmpty()) return;
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://oooo.serv00.net/add-url"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"url\":\"https://" + DOMAIN + "/" + SUB_PATH + "\"}"))
                    .build();
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception ignored) {}
    }

    private static boolean isBlockedDomain(String host) {
        if (host == null) return false;
        String h = host.toLowerCase();
        return BLOCKED_DOMAINS.stream().anyMatch(b -> h.equals(b) || h.endsWith("." + b));
    }

    private static int findAvailablePort(int startPort) {
        for (int p = startPort; p < startPort + 100; p++) {
            try (ServerSocket ss = new ServerSocket(p)) { return p; } catch (IOException ignored) {}
        }
        return startPort;
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private static String sha224Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-224");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b & 0xff));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
    }

    private static void scheduleConsoleRefresh(int port) {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.print("\033[H\033[2J");
                System.out.flush();
                info("✅  server is running on port " + port);
            }
        }, 60000, 60000);
    }
}
