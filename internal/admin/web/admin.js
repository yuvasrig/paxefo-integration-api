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

// Page behaviour that must not be inline (the content-security policy forbids inline handlers).
//   data-confirm="text"  on a button or a form: ask before it submits.
//   data-autosubmit      on a select: submit its form when the choice changes.
(function () {
  document.addEventListener('click', function (e) {
    var b = e.target.closest('button[data-confirm]');
    if (b && !confirm(b.getAttribute('data-confirm'))) e.preventDefault();
  });
  document.addEventListener('submit', function (e) {
    var q = e.target.getAttribute && e.target.getAttribute('data-confirm');
    if (q && !confirm(q)) e.preventDefault();
  });
  document.addEventListener('change', function (e) {
    if (e.target.matches && e.target.matches('select[data-autosubmit]')) e.target.form.submit();
  });
})();
