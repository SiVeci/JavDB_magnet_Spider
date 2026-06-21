package com.javdb_spider.app;

import android.webkit.CookieManager;
import android.webkit.WebSettings;
import android.webkit.WebView;

/**
 * 统一的 WebView 配置与安全加固。
 *
 * <p>MainActivity 的登录 WebView 与 SpiderService 的隐身抓取 WebView 此前各自重复设置
 * JS / DOM storage / 第三方 Cookie，且都未关闭 file:// 访问。这里集中处理，保证两端一致。
 *
 * <p>安全取舍：
 * <ul>
 *   <li>两个 WebView 只加载 http(s) 网页，无需任何 file:// / content:// 访问，
 *       故显式关闭以缩小攻击面（纯收益，不影响功能）。</li>
 *   <li><b>保留</b>第三方 Cookie：javdb 登录跨域写 Cookie 依赖此项，关闭会破坏登录采集。</li>
 * </ul>
 */
public final class WebViewConfig {

    private WebViewConfig() {
    }

    /** 按统一策略配置并加固给定 WebView。 */
    public static void configure(WebView webView) {
        WebSettings settings = webView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);

        // 安全加固：本应用的 WebView 只访问远程网页，关闭一切本地文件访问。
        settings.setAllowFileAccess(false);
        settings.setAllowContentAccess(false);
        settings.setAllowFileAccessFromFileURLs(false);
        settings.setAllowUniversalAccessFromFileURLs(false);

        // 保留第三方 Cookie：javdb 登录依赖跨域 Cookie。
        CookieManager.getInstance().setAcceptThirdPartyCookies(webView, true);
    }
}
