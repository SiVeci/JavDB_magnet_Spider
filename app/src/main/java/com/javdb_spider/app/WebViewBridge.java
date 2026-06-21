package com.javdb_spider.app;

import android.webkit.CookieManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WebViewBridge {
    private static final long FETCH_TIMEOUT_SECONDS = 45;

    // 保存当前正在运行的后台服务实例
    // volatile：主线程写入、后台线程读取，保证跨线程可见性
    public static volatile SpiderService activeService = null;

    // 后端 uvicorn 是否已开始监听端口。
    // 服务在端口就绪后置 true；UI 据此避免在后端就绪前打开控制台导致连接失败。
    public static volatile boolean backendReady = false;

    public interface HtmlCallback {
        void onResult(String html);
    }

    /**
     * 这个方法就是刚才在 Python 里通过 jclass 调用的 getHtmlBlocking()
     */
    public static String getHtmlBlocking(String url) {
        if (activeService == null) {
            return "<html><body>Engine Error: SpiderService not running</body></html>";
        }

        // 用数组绕过 lambda 表达式的 final 限制
        final String[] result = new String[1];
        // 创建一个倒数锁，初始值为 1
        CountDownLatch latch = new CountDownLatch(1);

        // 通知安卓主线程的 Service 去加载网页
        activeService.fetchHtml(url, new HtmlCallback() {
            @Override
            public void onResult(String html) {
                result[0] = html;
                latch.countDown(); // 网页加载完，锁减 1，放行 Python 线程
            }
        });

        try {
            // Python 线程运行到这里会被挂起阻塞，直到 latch 被 countDown
            boolean completed = latch.await(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!completed) {
                return "<html><body>Engine Timeout: WebView fetch exceeded 45 seconds</body></html>";
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "<html><body>Engine Error: WebView fetch interrupted</body></html>";
        }

        return result[0] != null ? result[0] : "";
    }

    public static String getJavdbCookie() {
        try {
            String cookie = CookieManager.getInstance().getCookie("https://javdb.com");
            return cookie != null ? cookie : "";
        } catch (Exception e) {
            return "";
        }
    }
}
