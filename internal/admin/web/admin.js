// Refreshes the element marked data-live-url with the HTML fragment the server returns.
(function () {
  var el = document.querySelector('[data-live-url]');
  if (!el) return;
  var url = el.getAttribute('data-live-url');
  var every = parseInt(el.getAttribute('data-interval'), 10) || 3000;
  function tick() {
    if (document.hidden) return;
    fetch(url, { credentials: 'same-origin', headers: { 'Accept': 'text/html' } })
      .then(function (r) {
        if (r.status === 401 || r.redirected) { location.reload(); return null; }
        return r.ok ? r.text() : null;
      })
      .then(function (html) { if (html) el.innerHTML = html; })
      .catch(function () {});
  }
  setInterval(tick, every);
})();
