package com.javdb_spider.app;

/**
 * 跨组件共享的常量：后端端口、监听地址与本机/局域网 URL。
 * 此前 8000 端口与 127.0.0.1 / 0.0.0.0 散落在 MainActivity 与 SpiderService 中，统一收敛于此。
 */
public final class Constants {

    private Constants() {
    }

    /** Python 后端（uvicorn）监听端口。 */
    public static final int BACKEND_PORT = 8000;

    /** 仅本机访问时的监听地址。 */
    public static final String HOST_LOCAL = "127.0.0.1";

    /** 局域网访问时的监听地址（所有网卡）。 */
    public static final String HOST_LAN = "0.0.0.0";

    /** 本机控制台地址，例如 http://127.0.0.1:8000 。 */
    public static String localBaseUrl() {
        return "http://" + HOST_LOCAL + ":" + BACKEND_PORT;
    }

    /** 给定局域网 IP 的控制台地址，例如 http://192.168.1.5:8000 。 */
    public static String lanBaseUrl(String ip) {
        return "http://" + ip + ":" + BACKEND_PORT;
    }
}
