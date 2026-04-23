package com.javdb_spider.app;

import android.Manifest;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.provider.Settings;
import android.view.View;
import android.webkit.CookieManager;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

public class MainActivity extends AppCompatActivity {

    private static final int REQUEST_CODE_OVERLAY = 1001;
    private static final int REQUEST_CODE_NOTIFICATION = 1002;

    private View controlPanel;
    private View loginContainer;
    private WebView visibleWebView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        controlPanel = findViewById(R.id.control_panel);
        loginContainer = findViewById(R.id.login_container);
        visibleWebView = findViewById(R.id.visible_webview);

        Button btnLogin = findViewById(R.id.btn_login);
        Button btnStart = findViewById(R.id.btn_start);
        Button btnOpenBrowser = findViewById(R.id.btn_open_browser); // 新增的按钮
        Button btnCloseLogin = findViewById(R.id.btn_close_login);
        Button btnCopyLink = findViewById(R.id.btn_copy_link); // 新增：复制当前链接按钮

        initVisibleWebView();

        btnLogin.setOnClickListener(v -> {
            controlPanel.setVisibility(View.GONE);
            loginContainer.setVisibility(View.VISIBLE);
            visibleWebView.loadUrl("https://javdb.com/login");
        });

        btnCloseLogin.setOnClickListener(v -> {
            loginContainer.setVisibility(View.GONE);
            controlPanel.setVisibility(View.VISIBLE);
            visibleWebView.loadUrl("about:blank");
            Toast.makeText(this, "Cookie 已自动接管", Toast.LENGTH_SHORT).show();
        });
        
        // 【新增】点击复制当前可见 WebView 的链接
        if (btnCopyLink != null) {
            btnCopyLink.setOnClickListener(v -> {
                String currentUrl = visibleWebView.getUrl();
                if (currentUrl != null && !currentUrl.isEmpty()) {
                    ClipboardManager clipboard = (ClipboardManager) getSystemService(Context.CLIPBOARD_SERVICE);
                    ClipData clip = ClipData.newPlainText("JavDB_URL", currentUrl);
                    clipboard.setPrimaryClip(clip);
                    Toast.makeText(this, "✅ 链接已复制，请前往控制台粘贴", Toast.LENGTH_LONG).show();
                } else {
                    Toast.makeText(this, "⚠️ 当前页面暂无有效链接", Toast.LENGTH_SHORT).show();
                }
            });
        }

        // 点击启动引擎
        btnStart.setOnClickListener(v -> {
            checkNotificationPermission();
        });

        // 【新增】点击直接跳转外部浏览器
        btnOpenBrowser.setOnClickListener(v -> {
            try {
                // 使用隐式 Intent 唤起系统默认浏览器
                Intent browserIntent = new Intent(Intent.ACTION_VIEW, Uri.parse("http://127.0.0.1:8000"));
                startActivity(browserIntent);
            } catch (Exception e) {
                Toast.makeText(this, "无法调用浏览器，请确保手机已安装浏览器App", Toast.LENGTH_SHORT).show();
            }
        });
    }

    private void initVisibleWebView() {
        WebSettings settings = visibleWebView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        CookieManager.getInstance().setAcceptThirdPartyCookies(visibleWebView, true);

        visibleWebView.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, String url) {
                view.loadUrl(url);
                return true;
            }
        });
    }

    // 1. 先检查通知权限
    private void checkNotificationPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
                ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.POST_NOTIFICATIONS}, REQUEST_CODE_NOTIFICATION);
                return;
            }
        }
        checkOverlayPermission();
    }

    // 2. 处理通知权限申请结果
    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQUEST_CODE_NOTIFICATION) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                checkOverlayPermission();
            } else {
                Toast.makeText(this, "必须授予通知权限，否则服务会被系统强杀！", Toast.LENGTH_LONG).show();
            }
        }
    }

    // 3. 检查悬浮窗权限
    private void checkOverlayPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && !Settings.canDrawOverlays(this)) {
            Toast.makeText(this, "必须授予悬浮窗权限才能在后台息屏爬取！", Toast.LENGTH_LONG).show();
            Intent intent = new Intent(Settings.ACTION_MANAGE_OVERLAY_PERMISSION,
                    Uri.parse("package:" + getPackageName()));
            startActivityForResult(intent, REQUEST_CODE_OVERLAY);
        } else {
            startEngineService();
        }
    }

    // 4. 处理悬浮窗权限申请结果
    @Override
    protected void onActivityResult(int requestCode, int resultCode, @Nullable Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == REQUEST_CODE_OVERLAY) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && Settings.canDrawOverlays(this)) {
                startEngineService();
            } else {
                Toast.makeText(this, "悬浮窗权限被拒绝，无法启动引擎", Toast.LENGTH_SHORT).show();
            }
        }
    }

    // ================= 正式启动服务 =================
    private void startEngineService() {
        Intent serviceIntent = new Intent(this, SpiderService.class);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(serviceIntent);
        } else {
            startService(serviceIntent);
        }

        Toast.makeText(this, "🚀 引擎已启动，可以点击第三步打开控制台了", Toast.LENGTH_SHORT).show();
        // 【修改】去掉了 moveTaskToBack(true); 这样界面就不会自动回退到桌面了
    }
}