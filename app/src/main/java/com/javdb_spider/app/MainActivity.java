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
import android.view.View;
import android.view.ViewGroup;
import android.view.Window;
import android.view.WindowManager;
import android.webkit.WebResourceRequest;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.TextView;
import android.widget.Toast;

import androidx.activity.OnBackPressedCallback;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

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

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            Window window = getWindow();
            window.addFlags(WindowManager.LayoutParams.FLAG_DRAWS_SYSTEM_BAR_BACKGROUNDS);
            window.setStatusBarColor(ContextCompat.getColor(this, R.color.background_primary));
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
        Button btnOpenBrowser = findViewById(R.id.btn_open_browser);
        Button btnCloseLogin = findViewById(R.id.btn_close_login);
        Button btnCopyLink = findViewById(R.id.btn_copy_link);

        SharedPreferences prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE);
        cbLanAccess.setChecked(prefs.getBoolean(PREF_LAN_ACCESS, false));
        cbLanAccess.setOnCheckedChangeListener((btn, checked) ->
                prefs.edit().putBoolean(PREF_LAN_ACCESS, checked).apply());

        initVisibleWebView();

        if (WebViewBridge.activeService != null) {
            setEngineStatusRunning();
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
            Toast.makeText(this, R.string.cookie_auto_captured, Toast.LENGTH_SHORT).show();
        });

        if (btnCopyLink != null) {
            btnCopyLink.setOnClickListener(v -> {
                String currentUrl = visibleWebView.getUrl();
                if (currentUrl != null && !currentUrl.isEmpty()) {
                    ClipboardManager clipboard = (ClipboardManager) getSystemService(Context.CLIPBOARD_SERVICE);
                    ClipData clip = ClipData.newPlainText("JavDB_URL", currentUrl);
                    clipboard.setPrimaryClip(clip);
                    Toast.makeText(this, R.string.link_copied, Toast.LENGTH_LONG).show();
                } else {
                    Toast.makeText(this, R.string.page_not_javdb, Toast.LENGTH_SHORT).show();
                }
            });
        }

        btnStart.setOnClickListener(v -> checkNotificationPermission());

        btnOpenBrowser.setOnClickListener(v -> {
            if (!WebViewBridge.backendReady) {
                Toast.makeText(this, R.string.backend_not_ready, Toast.LENGTH_SHORT).show();
                return;
            }
            try {
                Intent browserIntent = new Intent(Intent.ACTION_VIEW, Uri.parse(Constants.localBaseUrl()));
                startActivity(browserIntent);
            } catch (Exception e) {
                Toast.makeText(this, R.string.no_browser_found, Toast.LENGTH_SHORT).show();
            }
        });

        registerBackHandler();
    }

    private void initVisibleWebView() {
        WebViewConfig.configure(visibleWebView);

        visibleWebView.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                view.loadUrl(request.getUrl().toString());
                return true;
            }
        });
    }

    private void checkNotificationPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
                && ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(
                    this,
                    new String[]{Manifest.permission.POST_NOTIFICATIONS},
                    REQUEST_CODE_NOTIFICATION);
            return;
        }
        checkOverlayPermission();
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQUEST_CODE_NOTIFICATION) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                checkOverlayPermission();
            } else {
                Toast.makeText(this, R.string.notification_permission_required, Toast.LENGTH_LONG).show();
            }
        }
    }

    private void checkOverlayPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && !Settings.canDrawOverlays(this)) {
            Toast.makeText(this, R.string.overlay_permission_required, Toast.LENGTH_LONG).show();
            Intent intent = new Intent(
                    Settings.ACTION_MANAGE_OVERLAY_PERMISSION,
                    Uri.parse("package:" + getPackageName()));
            startActivityForResult(intent, REQUEST_CODE_OVERLAY);
        } else {
            startEngineService();
        }
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, @Nullable Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == REQUEST_CODE_OVERLAY) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && Settings.canDrawOverlays(this)) {
                startEngineService();
            } else {
                Toast.makeText(this, R.string.overlay_permission_denied, Toast.LENGTH_SHORT).show();
            }
        }
    }

    private void startEngineService() {
        boolean lanMode = cbLanAccess.isChecked();
        Intent serviceIntent = new Intent(this, SpiderService.class);
        serviceIntent.putExtra("lan_mode", lanMode);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(serviceIntent);
        } else {
            startService(serviceIntent);
        }

        cbLanAccess.setEnabled(false);
        Toast.makeText(this, R.string.engine_started_browser, Toast.LENGTH_SHORT).show();
        setEngineStatusRunning();

        if (lanMode) {
            showLanAddress();
        }
    }

    private void showLanAddress() {
        String ip = getLocalIpAddress();
        if (ip != null) {
            String lanUrl = Constants.lanBaseUrl(ip);
            tvLanAddress.setText(getString(R.string.lan_address, ip));
            tvLanAddress.setVisibility(View.VISIBLE);
            tvLanAddress.setOnClickListener(v -> {
                ClipboardManager clipboard = (ClipboardManager) getSystemService(Context.CLIPBOARD_SERVICE);
                clipboard.setPrimaryClip(ClipData.newPlainText("lan_url", lanUrl));
                Toast.makeText(this, R.string.lan_address_copied, Toast.LENGTH_SHORT).show();
            });
        } else {
            tvLanAddress.setText(R.string.wlan_unavailable);
            tvLanAddress.setVisibility(View.VISIBLE);
        }
    }

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

    private void setEngineStatusRunning() {
        if (tvEngineStatus != null) {
            tvEngineStatus.setText(R.string.engine_status_running);
            tvEngineStatus.setTextColor(ContextCompat.getColor(this, R.color.accent_success));
        }
        if (cbLanAccess != null) {
            cbLanAccess.setEnabled(false);
        }
    }

    private void registerBackHandler() {
        getOnBackPressedDispatcher().addCallback(this, new OnBackPressedCallback(true) {
            @Override
            public void handleOnBackPressed() {
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
                setEnabled(false);
                getOnBackPressedDispatcher().onBackPressed();
                setEnabled(true);
            }
        });
    }

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
