// Telegram WebApp auto-authentication for /login.
// Runs when the page is opened inside Telegram's WebApp (MenuButtonWebApp);
// posts initData to /auth/webapp which sets the session cookie server-side
// (HttpOnly), then we redirect to /.
// Extracted out of an inline <script> so CSP can drop `'unsafe-inline'`
// from script-src.
(function () {
    var tg = window.Telegram && window.Telegram.WebApp;
    if (!tg || !tg.initData) return; // not in WebApp — let the Login Widget handle it

    var loginWidget = document.querySelector('.telegram-login');
    var infoText = document.getElementById('info-text');
    var loadingDiv = document.getElementById('webapp-loading');

    if (loginWidget) loginWidget.style.display = 'none';
    if (infoText) infoText.style.display = 'none';
    if (loadingDiv) loadingDiv.style.display = 'block';

    tg.expand();

    fetch('/auth/webapp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        // same-origin is the default; explicit for robustness if API ever moves origins.
        credentials: 'same-origin',
        body: JSON.stringify({ initData: tg.initData }),
    })
    .then(function (response) { return response.json().then(function (d) { return { ok: response.ok, data: d }; }); })
    .then(function (res) {
        var data = res.data || {};
        if (res.ok && data.success) {
            // Cookie was set server-side with HttpOnly — JS doesn't touch it.
            window.location.href = '/';
            return;
        }

        console.error('WebApp auth failed:', data.error);
        if (loadingDiv) loadingDiv.style.display = 'none';

        if (data.status === 'pending') {
            var pending = document.querySelector('.pending-message');
            if (pending) pending.style.removeProperty('display');
        } else if (data.status === 'denied' || data.status === 'frozen') {
            var denied = document.querySelector('.denied-message');
            if (denied) denied.style.removeProperty('display');
        } else {
            var errorDiv = document.createElement('div');
            errorDiv.className = 'error-message';
            errorDiv.textContent = data.error || 'Authentication failed. Please try in browser.';
            var container = document.querySelector('.login-container');
            if (container) container.insertBefore(errorDiv, document.querySelector('.telegram-login'));
        }

        if (infoText) {
            infoText.innerHTML = 'WebApp authentication failed.<br>Please open in browser or contact admin.';
            infoText.style.display = 'block';
        }
    })
    .catch(function (error) {
        console.error('WebApp auth error:', error);
        if (loadingDiv) loadingDiv.style.display = 'none';
        if (loginWidget) loginWidget.style.display = 'block';
        if (infoText) infoText.style.display = 'block';
    });
})();
