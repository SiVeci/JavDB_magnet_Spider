package com.javdb_spider.app;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.graphics.PixelFormat;
import android.os.Build;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.Looper;
import android.util.Log;
import android.view.Gravity;
import android.view.WindowManager;
import android.webkit.ValueCallback;
import android.webkit.WebView;
import android.webkit.WebViewClient;

import com.chaquo.python.PyException;
import com.chaquo.python.PyObject;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

public class SpiderService extends Service {
    private static final long FETCH_TIMEOUT_MS = 45000L;
    private static final String CHANNEL_ID = "spider_channel";
    private static final String ACTION_STOP_SERVICE = "com.javdb_spider.app.STOP_SERVICE";

    // 后端就绪探测：轮询本机端口直到 uvicorn 开始监听。
    private static final long BACKEND_READY_TIMEOUT_MS = 30000L;
    private static final long BACKEND_READY_POLL_MS = 400L;
    private static final int BACKEND_READY_CONNECT_TIMEOUT_MS = 800;

    private WindowManager windowManager;
    private WebView stealthWebView;
    private Handler mainHandler;
    private Runnable statusUpdater;
    private HandlerThread statusThread;
    private Handler statusHandler;

    private boolean lanMode = false;
    private boolean backendStarted = false;

    @Override
    public void onCreate() {
        super.onCreate();
        mainHandler = new Handler(Looper.getMainLooper());

        statusThread = new HandlerThread("spider-status");
        statusThread.start();
        statusHandler = new Handler(statusThread.getLooper());

        WebViewBridge.activeService = this;
        WebViewBridge.backendReady = false;
        startForeground(1, createNotification());
        initStealthWebView();
        startStatusUpdater();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && ACTION_STOP_SERVICE.equals(intent.getAction())) {
            stopSelf();
            return START_NOT_STICKY;
        }

        if (intent != null) {
            lanMode = intent.getBooleanExtra("lan_mode", false);
            notifyForeground(createNotification());
        }

        if (!backendStarted) {
            backendStarted = true;
            startPythonBackend();
            startBackendReadyProbe();
        }
        return START_STICKY;
    }

    private void initStealthWebView() {
        windowManager = (WindowManager) getSystemService(WINDOW_SERVICE);
        stealthWebView = new WebView(this);

        WebViewConfig.configure(stealthWebView);

        WindowManager.LayoutParams params = new WindowManager.LayoutParams(
                1,
                1,
                WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY,
                WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE | WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL,
                PixelFormat.TRANSLUCENT
        );
        params.gravity = Gravity.TOP | Gravity.START;
        windowManager.addView(stealthWebView, params);
    }

    public void fetchHtml(String url, WebViewBridge.HtmlCallback callback) {
        if (url == null || (!url.startsWith("http://") && !url.startsWith("https://"))) {
            callback.onResult(null);
            return;
        }

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
                                    if (html.startsWith("\"") && html.endsWith("\"")) {
                                        html = html.substring(1, html.length() - 1)
                                                .replace("\\u003C", "<")
                                                .replace("\\\"", "\"");
                                    }
                                    callback.onResult(html);
                                }
                            });
                }
            });

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
        final String host = lanMode ? Constants.HOST_LAN : Constants.HOST_LOCAL;
        new Thread(() -> {
            try {
                if (!Python.isStarted()) {
                    Python.start(new AndroidPlatform(getApplicationContext()));
                }
                Python py = Python.getInstance();
                PyObject mainModule = py.getModule("main");

                Log.d("PythonSpider", getString(R.string.python_starting_log, host));
                mainModule.callAttr("start_server", host);
                Log.d("PythonSpider", getString(R.string.python_stopped_log));
            } catch (PyException e) {
                Log.e("PythonSpider", getString(R.string.python_crashed_log) + "\n" + e.getMessage(), e);
                onPythonBackendStopped(getString(R.string.python_crashed_reason));
            } catch (Exception e) {
                Log.e("PythonSpider", getString(R.string.python_interrupted_log) + "\n" + e.getMessage(), e);
                onPythonBackendStopped(getString(R.string.python_interrupted_reason));
            }
        }).start();
    }

    /**
     * 轮询本机端口直到 uvicorn 开始监听，置 {@link WebViewBridge#backendReady} 并刷新通知。
     * uvicorn.run 会阻塞后端线程，无法直接回报就绪，故用独立探测线程。
     */
    private void startBackendReadyProbe() {
        new Thread(() -> {
            long deadline = System.currentTimeMillis() + BACKEND_READY_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                // 服务已销毁（手动停止 / 后端退出）则放弃探测，避免发出过期通知。
                if (!backendStarted || WebViewBridge.activeService != this) {
                    return;
                }
                try (java.net.Socket socket = new java.net.Socket()) {
                    socket.connect(
                            new java.net.InetSocketAddress(Constants.HOST_LOCAL, Constants.BACKEND_PORT),
                            BACKEND_READY_CONNECT_TIMEOUT_MS);
                    WebViewBridge.backendReady = true;
                    mainHandler.post(() -> notifyForeground(createNotification()));
                    return;
                } catch (Exception probing) {
                    try {
                        Thread.sleep(BACKEND_READY_POLL_MS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, "spider-ready-probe").start();
    }

    private void onPythonBackendStopped(String reason) {
        WebViewBridge.activeService = null;
        WebViewBridge.backendReady = false;
        backendStarted = false;
        notifyForeground(buildNotification(
                getString(R.string.notification_backend_stopped),
                getString(R.string.notification_backend_restart_hint, reason),
                false
        ));
        // 后端已退出，前台服务不再有存在意义：清理自身，避免遗留僵尸前台服务。
        stopSelf();
    }

    private Notification createNotification() {
        if (!WebViewBridge.backendReady) {
            return buildNotification(
                    getString(R.string.notification_title_initializing),
                    getString(R.string.notification_initializing_text),
                    true
            );
        }
        return buildNotification(
                getString(R.string.notification_title_running),
                lanMode ? getString(R.string.notification_lan_text) : getString(R.string.notification_local_text),
                true
        );
    }

    private Notification buildNotification(String title, String text, boolean ongoing) {
        NotificationManager nm = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                    CHANNEL_ID,
                    getString(R.string.notification_channel_name),
                    NotificationManager.IMPORTANCE_LOW);
            nm.createNotificationChannel(channel);
        }

        Intent openIntent = new Intent(this, MainActivity.class);
        openIntent.setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP);
        PendingIntent openPendingIntent = PendingIntent.getActivity(
                this,
                0,
                openIntent,
                PendingIntent.FLAG_IMMUTABLE);

        Intent stopIntent = new Intent(this, SpiderService.class);
        stopIntent.setAction(ACTION_STOP_SERVICE);
        PendingIntent stopPendingIntent = PendingIntent.getService(
                this,
                1,
                stopIntent,
                PendingIntent.FLAG_IMMUTABLE);

        Notification.Builder builder = Build.VERSION.SDK_INT >= Build.VERSION_CODES.O
                ? new Notification.Builder(this, CHANNEL_ID)
                : new Notification.Builder(this);

        builder.setContentTitle(title)
                .setContentText(text)
                .setSmallIcon(android.R.drawable.ic_menu_compass)
                .setContentIntent(openPendingIntent)
                .setOnlyAlertOnce(true)
                .setOngoing(ongoing);

        if (ongoing) {
            builder.addAction(
                    android.R.drawable.ic_menu_close_clear_cancel,
                    getString(R.string.notification_stop_action),
                    stopPendingIntent);
        }

        return builder.build();
    }

    private void notifyForeground(Notification notification) {
        NotificationManager nm = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        nm.notify(1, notification);
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
            if (!statusFile.exists()) {
                return;
            }

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

            String title = getString(R.string.notification_title_running);
            String text = lanMode ? getString(R.string.notification_lan_text) : getString(R.string.notification_local_text);

            switch (state) {
                case "running":
                    title = getString(R.string.notification_crawling_title, progress);
                    text = getString(R.string.notification_current, current);
                    break;
                case "paused_need_cookie":
                case "paused_need_choice":
                    title = getString(R.string.notification_waiting_title);
                    text = getString(R.string.notification_waiting_text, current);
                    break;
                case "finished":
                    title = getString(R.string.notification_finished_title);
                    text = getString(R.string.notification_finished_text);
                    break;
                case "error":
                    title = getString(R.string.notification_error_title);
                    text = getString(R.string.notification_error_text);
                    break;
                case "stopped":
                    title = getString(R.string.notification_stopped_title);
                    text = getString(R.string.notification_stopped_text);
                    break;
            }

            notifyForeground(buildNotification(title, text, true));
        } catch (Exception ignored) {
        }
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        WebViewBridge.activeService = null;
        WebViewBridge.backendReady = false;
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
            stealthWebView = null;
        }
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }
}
