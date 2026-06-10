package com.javdb_spider.app;

import android.Manifest;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.provider.Settings;
import android.graphics.Color;
import android.view.View;
import android.view.ViewGroup;
import android.view.Window;
import android.view.WindowManager;
import android.webkit.CookieManager;
import android.webkit.WebResourceRequest;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.Toast;
import android.widget.TextView;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import com.google.android.material.card.MaterialCardView;
import androidx.activity.OnBackPressedCallback;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

public class MainActivity extends AppCompatActivity {

    private static final int REQUEST_CODE_OVERLAY = 1001;
    private static final int REQUEST_CODE_NOTIFICATION = 1002;

    private static final String PREFS_NAME = "spider_prefs";
    private static final String PREF_LAN_ACCESS = "lan_access";

    private View controlPanel;
    private View loginContainer;
    private WebView visibleWebView;
    private TextView tvEngineStatus;
    private CheckBox cbLanAccess;
    private TextView tvLanAddress;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // 适配浅色状态栏：将状态栏背景设为与App背景一致的浅灰，并将状态栏图标/文字反转为深色
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            Window window = getWindow();
            window.addFlags(WindowManager.LayoutParams.FLAG_DRAWS_SYSTEM_BAR_BACKGROUNDS);
            window.setStatusBarColor(Color.parseColor("#F3F4F6"));
            window.getDecorView().setSystemUiVisibility(View.SYSTEM_UI_FLAG_LIGHT_STATUS_BAR);
        }

        controlPanel = findViewById(R.id.control_panel);
        loginContainer = findViewById(R.id.login_container);
        visibleWebView = findViewById(R.id.visible_webview);
        tvEngineStatus = findViewById(R.id.tv_engine_status);
        cbLanAccess = findViewById(R.id.cb_lan_access);
        tvLanAddress = findViewById(R.id.tv_lan_address);

        Button btnLogin = findViewById(R.id.btn_login);
        Button btnStart = findViewById(R.id.btn_start);
        Button btnOpenBrowser = findViewById(R.id.btn_open_browser); // 新增的按钮
        Button btnCloseLogin = findViewById(R.id.btn_close_login);
        Button btnCopyLink = findViewById(R.id.btn_copy_link); // 新增：复制当前链接按钮

        // 恢复局域网访问勾选状态
        SharedPreferences prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE);
        cbLanAccess.setChecked(prefs.getBoolean(PREF_LAN_ACCESS, false));
        cbLanAccess.setOnCheckedChangeListener((btn, checked) ->
                prefs.edit().putBoolean(PREF_LAN_ACCESS, checked).apply());

        initVisibleWebView();

        // 如果由于应用切到后台再回来，服务仍在运行，自动恢复绿灯状态
        if (WebViewBridge.activeService != null) {
            setEngineStatusRunning();
            // 恢复局域网地址显示
            if (prefs.getBoolean(PREF_LAN_ACCESS, false)) {
                showLanAddress();
            }
        }

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

        // 注册返回键处理（兼容 Android 13+ 预测式返回手势）
        registerBackHandler();
    }

    private void initVisibleWebView() {
        WebSettings settings = visibleWebView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        CookieManager.getInstance().setAcceptThirdPartyCookies(visibleWebView, true);

        visibleWebView.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                view.loadUrl(request.getUrl().toString());
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
        boolean lanMode = cbLanAccess.isChecked();
        Intent serviceIntent = new Intent(this, SpiderService.class);
        serviceIntent.putExtra("lan_mode", lanMode);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(serviceIntent);
        } else {
            startService(serviceIntent);
        }

        // 启动后禁用勾选框，避免运行中切换状态
        cbLanAccess.setEnabled(false);

        Toast.makeText(this, "🚀 引擎已启动，可以点击第三步打开控制台了", Toast.LENGTH_SHORT).show();
        setEngineStatusRunning();

        // 如果开启局域网模式，展示本机 IP
        if (lanMode) {
            showLanAddress();
        }
    }

    // 显示局域网访问地址
    private void showLanAddress() {
        String ip = getLocalIpAddress();
        if (ip != null) {
            tvLanAddress.setText("局域网地址: http://" + ip + ":8000  （点击复制）");
            tvLanAddress.setVisibility(View.VISIBLE);
            tvLanAddress.setOnClickListener(v -> {
                ClipboardManager clipboard = (ClipboardManager) getSystemService(Context.CLIPBOARD_SERVICE);
                clipboard.setPrimaryClip(ClipData.newPlainText("lan_url", "http://" + ip + ":8000"));
                Toast.makeText(this, "已复制局域网地址", Toast.LENGTH_SHORT).show();
            });
        } else {
            tvLanAddress.setText("局域网地址获取失败，请确认已连接 WiFi");
            tvLanAddress.setVisibility(View.VISIBLE);
        }
    }

    // 获取本机 WiFi/以太网 IPv4 地址（无需额外权限）
    private String getLocalIpAddress() {
        try {
            for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
                 en.hasMoreElements(); ) {
                NetworkInterface intf = en.nextElement();
                for (Enumeration<InetAddress> addrs = intf.getInetAddresses();
                     addrs.hasMoreElements(); ) {
                    InetAddress addr = addrs.nextElement();
                    if (!addr.isLoopbackAddress() && addr instanceof java.net.Inet4Address) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    // 引擎运行中的 UI 状态
    private void setEngineStatusRunning() {
        if (tvEngineStatus != null) {
            tvEngineStatus.setText("🟢 引擎状态：运行中");
            tvEngineStatus.setTextColor(Color.parseColor("#10B981"));
        }
        if (cbLanAccess != null) {
            cbLanAccess.setEnabled(false);
        }
    }

    // ================= 返回键处理 =================
    // targetSdk 36 + Android 13+ 预测式返回手势下，旧的 onBackPressed() 不再被回调，
    // 必须改用 OnBackPressedDispatcher 注册 callback 才能拦截返回。
    private void registerBackHandler() {
        getOnBackPressedDispatcher().addCallback(this, new OnBackPressedCallback(true) {
            @Override
            public void handleOnBackPressed() {
                // 登录页可见时，返回键回到控制台而非退出应用
                if (loginContainer != null && loginContainer.getVisibility() == View.VISIBLE) {
                    loginContainer.setVisibility(View.GONE);
                    if (controlPanel != null) {
                        controlPanel.setVisibility(View.VISIBLE);
                    }
                    if (visibleWebView != null) {
                        visibleWebView.loadUrl("about:blank");
                    }
                    return;
                }
                // 非登录页：临时禁用本回调并把返回事件交还系统（执行默认退出）
                setEnabled(false);
                getOnBackPressedDispatcher().onBackPressed();
                setEnabled(true);
            }
        });
    }

    // ================= WebView 生命周期管理（防内存泄漏） =================
    @Override
    protected void onPause() {
        if (visibleWebView != null) {
            visibleWebView.onPause();
        }
        super.onPause();
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (visibleWebView != null) {
            visibleWebView.onResume();
        }
    }

    @Override
    protected void onDestroy() {
        if (visibleWebView != null) {
            // 先从父容器移除，断开视图树引用，再销毁，彻底避免 WebView 泄漏 Activity
            ViewGroup parent = (ViewGroup) visibleWebView.getParent();
            if (parent != null) {
                parent.removeView(visibleWebView);
            }
            visibleWebView.stopLoading();
            visibleWebView.setWebViewClient(null);
            visibleWebView.destroy();
            visibleWebView = null;
        }
        super.onDestroy();
    }
}