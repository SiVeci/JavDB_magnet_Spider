package com.javdb_spider.app;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.graphics.PixelFormat;
import android.os.Build;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.Looper;
import android.view.Gravity;
import android.view.WindowManager;
import android.webkit.CookieManager;
import android.webkit.ValueCallback;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;

import com.chaquo.python.PyObject;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;

import android.util.Log;
import com.chaquo.python.PyException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONObject;

public class SpiderService extends Service {
    private static final long FETCH_TIMEOUT_MS = 45000L;

    private WindowManager windowManager;
    private WebView stealthWebView;
    private Handler mainHandler;
    private Runnable statusUpdater;
    // 后台线程：用于状态文件 I/O，避免在主线程读 status.json 造成 ANR
    private HandlerThread statusThread;
    private Handler statusHandler;

    // 局域网访问模式标志；由 Intent Extra 从 MainActivity 传入
    private boolean lanMode = false;
    // 防止服务因系统重启 (START_STICKY) 而重复启动 Python 后端
    private boolean backendStarted = false;

    @Override
    public void onCreate() {
        super.onCreate();
        mainHandler = new Handler(Looper.getMainLooper());

        // 后台线程处理状态文件读取，避免主线程 I/O 导致 ANR
        statusThread = new HandlerThread("spider-status");
        statusThread.start();
        statusHandler = new Handler(statusThread.getLooper());

        // 1. 注册桥梁，让 Python 认识这个 Service
        WebViewBridge.activeService = this;

        // 2. 启动前台常驻通知 (防止被系统杀后台)
        startForeground(1, createNotification());

        // 3. 初始化 1x1 像素隐藏浏览器
        initStealthWebView();

        // 4. Python 后端在 onStartCommand() 中启动（需要先从 Intent 读取 lan_mode）

        // 5. 启动通知栏进度同步
        startStatusUpdater();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null) {
            lanMode = intent.getBooleanExtra("lan_mode", false);
        }
        // 防止 START_STICKY 重启时重复启动 Python 后端
        if (!backendStarted) {
            backendStarted = true;
            startPythonBackend();
        }
        return START_STICKY;
    }

    private void initStealthWebView() {
        windowManager = (WindowManager) getSystemService(WINDOW_SERVICE);
        stealthWebView = new WebView(this);

        // 配置浏览器伪装环境
        WebSettings settings = stealthWebView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        // 强制接受第三方 Cookie，过 CF 盾必备
        CookieManager.getInstance().setAcceptThirdPartyCookies(stealthWebView, true);

        // 配置 1x1 像素的悬浮窗参数
        WindowManager.LayoutParams params = new WindowManager.LayoutParams(
                1, 1, // 宽高仅 1 像素
                Build.VERSION.SDK_INT >= Build.VERSION_CODES.O ?
                        WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY :
                        WindowManager.LayoutParams.TYPE_PHONE,
                WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE | WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL,
                PixelFormat.TRANSLUCENT
        );
        params.gravity = Gravity.TOP | Gravity.START;

        // 将浏览器挂载到屏幕外层
        windowManager.addView(stealthWebView, params);
    }

    /**
     * 接收 Python 的指令去抓取网页
     */
    public void fetchHtml(String url, WebViewBridge.HtmlCallback callback) {
        // 必须在主线程操作 UI
        mainHandler.post(() -> {
            final AtomicBoolean completed = new AtomicBoolean(false);
            final Runnable timeoutTask = () -> {
                if (completed.compareAndSet(false, true)) {
                    try {
                        stealthWebView.stopLoading();
                    } catch (Exception ignored) {
                    }
                    callback.onResult("<html><body>Engine Timeout: WebView fetch exceeded 45 seconds</body></html>");
                }
            };
            mainHandler.postDelayed(timeoutTask, FETCH_TIMEOUT_MS);

            stealthWebView.setWebViewClient(new WebViewClient() {
                @Override
                public void onPageFinished(WebView view, String currentUrl) {
                    // 页面加载完成后，注入 JS 提取整个网页的 HTML 源码
                    stealthWebView.evaluateJavascript(
                            "(function() { return '<html>'+document.getElementsByTagName('html')[0].innerHTML+'</html>'; })();",
                            new ValueCallback<String>() {
                                @Override
                                public void onReceiveValue(String html) {
                                    if (!completed.compareAndSet(false, true)) {
                                        return;
                                    }
                                    mainHandler.removeCallbacks(timeoutTask);
                                    if (html == null) {
                                        html = "";
                                    }
                                    // 过滤掉自带的转义引号
                                    if (html.startsWith("\"") && html.endsWith("\"")) {
                                        html = html.substring(1, html.length() - 1).replace("\\u003C", "<").replace("\\\"", "\"");
                                    }
                                    callback.onResult(html);
                                }
                            });
                }
            });
            // 开始加载
            try {
                stealthWebView.loadUrl(url);
            } catch (Exception e) {
                if (completed.compareAndSet(false, true)) {
                    mainHandler.removeCallbacks(timeoutTask);
                    callback.onResult("<html><body>Engine Error: WebView load failed</body></html>");
                }
            }
        });
    }

    private void startPythonBackend() {
        final String host = lanMode ? "0.0.0.0" : "127.0.0.1";
        new Thread(() -> {
            try {
                if (!Python.isStarted()) {
                    // 使用 getApplicationContext() 更加稳妥
                    Python.start(new AndroidPlatform(getApplicationContext()));
                }
                Python py = Python.getInstance();
                PyObject mainModule = py.getModule("main");

                Log.d("PythonSpider", "正在启动 Uvicorn 服务，host=" + host);
                mainModule.callAttr("start_server", host);
                Log.d("PythonSpider", "Uvicorn 服务安全退出");

            } catch (PyException e) {
                // 如果 Python 内部报错，不仅不会闪退，还会把详细报错打在控制台
                Log.e("PythonSpider", "❌ Python 引擎崩溃啦:\n" + e.getMessage(), e);
                onPythonBackendStopped("Python 引擎异常退出");
            } catch (Exception e) {
                Log.e("PythonSpider", "❌ 安卓系统底层拦截:\n" + e.getMessage(), e);
                onPythonBackendStopped("引擎被系统中断");
            }
        }).start();
    }

    /**
     * Python 后端线程结束（崩溃或 uvicorn 退出）时调用：
     * 解除桥接引用并更新通知告知用户，避免服务变成无人知晓的僵尸。
     * 不做自动重启，以免崩溃循环时反复拉起导致死循环。
     */
    private void onPythonBackendStopped(String reason) {
        WebViewBridge.activeService = null;
        try {
            NotificationManager nm = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
            Notification.Builder builder = Build.VERSION.SDK_INT >= Build.VERSION_CODES.O ?
                    new Notification.Builder(this, "spider_channel") : new Notification.Builder(this);
            Notification notification = builder.setContentTitle("❌ 引擎已停止")
                    .setContentText(reason + "，请回到 App 重新启动引擎")
                    .setSmallIcon(android.R.drawable.ic_menu_compass)
                    .setOnlyAlertOnce(true)
                    .build();
            nm.notify(1, notification);
        } catch (Exception ignored) {
            // 通知失败不应再抛出
        }
    }

    private Notification createNotification() {
        String channelId = "spider_channel";
        NotificationManager nm = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(channelId, "JavDB 引擎", NotificationManager.IMPORTANCE_LOW);
            nm.createNotificationChannel(channel);
        }
        Notification.Builder builder = Build.VERSION.SDK_INT >= Build.VERSION_CODES.O ?
                new Notification.Builder(this, channelId) : new Notification.Builder(this);

        return builder.setContentTitle("JavDB 爬虫引擎运行中")
                .setContentText(lanMode ? "局域网访问已开启，端口 8000" : "127.0.0.1:8000 已开启，请前往浏览器配置")
                .setSmallIcon(android.R.drawable.ic_menu_compass) // 临时用个系统图标
                .setOnlyAlertOnce(true) // 重要：避免每次更新通知时发出声音或震动
                .build();
    }

    private void startStatusUpdater() {
        statusUpdater = new Runnable() {
            @Override
            public void run() {
                updateNotificationFromJson();
                statusHandler.postDelayed(this, 2000);
            }
        };
        statusHandler.postDelayed(statusUpdater, 2000);
    }

    private void updateNotificationFromJson() {
        try {
            File statusFile = new File(getFilesDir(), "data/status.json");
            if (!statusFile.exists()) return;

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(statusFile), "UTF-8"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            }

            JSONObject json = new JSONObject(sb.toString());
            String state = json.optString("state", "idle");
            String progress = json.optString("progress", "");
            String current = json.optString("current", "");

            String title = "JavDB 引擎运行中";
            String text = lanMode ? "局域网访问已开启，端口 8000" : "127.0.0.1:8000 已开启，请前往控制台配置";

            switch (state) {
                case "running":
                    title = "▶️ 爬取中: " + progress;
                    text = "当前: " + current;
                    break;
                case "paused_need_cookie":
                case "paused_need_choice":
                    title = "⚠️ 任务已挂起等待救援";
                    text = "请打开控制台处理 (" + current + ")";
                    break;
                case "finished":
                    title = "✅ 任务已圆满完成";
                    text = "所有目标已抓取完毕，文件已保存";
                    break;
                case "error":
                    title = "❌ 任务异常终止";
                    text = "请打开控制台查看详细日志";
                    break;
                case "stopped":
                    title = "🛑 任务已手动停止";
                    text = "进度已安全保存";
                    break;
            }

            NotificationManager nm = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
            Notification.Builder builder = Build.VERSION.SDK_INT >= Build.VERSION_CODES.O ?
                    new Notification.Builder(this, "spider_channel") : new Notification.Builder(this);

            Notification notification = builder.setContentTitle(title)
                    .setContentText(text)
                    .setSmallIcon(android.R.drawable.ic_menu_compass)
                    .setOnlyAlertOnce(true) // 重要：静默更新
                    .build();

            nm.notify(1, notification);

        } catch (Exception e) {
            // 解析失败或文件正被 Python 占用时跳过本次更新，不打断服务
        }
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        WebViewBridge.activeService = null;
        if (statusUpdater != null && statusHandler != null) {
            statusHandler.removeCallbacks(statusUpdater);
        }
        if (statusThread != null) {
            statusThread.quitSafely();
            statusThread = null;
        }
        if (windowManager != null && stealthWebView != null) {
            windowManager.removeView(stealthWebView);
            stealthWebView.destroy();
        }
    }

    @Override
    public IBinder onBind(Intent intent) { return null; }
}
