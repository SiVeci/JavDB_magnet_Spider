package com.javdb_spider.app;

import java.util.concurrent.CountDownLatch;

public class WebViewBridge {

    // 保存当前正在运行的后台服务实例
    public static SpiderService activeService = null;

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
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return "";
        }

        return result[0];
    }
}