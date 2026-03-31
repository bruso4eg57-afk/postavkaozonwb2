/**
 * 1C API integration (optional module).
 */
function refreshOneCRaw() {
  Logger.log('Starting 1C refresh...');
  const baseUrl = getScriptProperty_(APP_CONFIG.PROPERTIES.ONEC_BASE_URL);
  const token = getScriptProperty_(APP_CONFIG.PROPERTIES.ONEC_API_TOKEN);

  if (!baseUrl || !token) {
    Logger.log('1C credentials are absent. Skipping 1C refresh without error.');
    return;
  }

  const rows = fetchOneCData_(baseUrl, token);
  fullRefreshSheet_(APP_CONFIG.SHEETS.ONEC_RAW, HEADERS.ONEC_RAW, rows);
  Logger.log('1C refresh done.');
}

function fetchOneCData_(baseUrl, token) {
  const timestamp = nowIso_();
  const url = baseUrl.replace(/\/$/, '') + '/api/stocks';

  const options = {
    method: 'get',
    headers: {
      Authorization: 'Bearer ' + token
    },
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const code = response.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error('1C API error. HTTP ' + code + ': ' + response.getContentText());
  }

  const items = safeJsonParse_(response.getContentText(), []);

  return items.map(function (item) {
    return [
      timestamp,
      normalizeText_(item.warehouse || ''),
      normalizeText_(item.article || ''),
      normalizeText_(item.name || ''),
      normalizeText_(item.model || ''),
      normalizeText_(item.color || ''),
      normalizeText_(item.size || ''),
      normalizeText_(item.barcode || ''),
      toNumber_(item.stock || 0),
      toNumber_(item.available || 0),
      toNumber_(item.inProduction || 0),
      normalizeText_(item.workshop || ''),
      '1C API'
    ];
  });
}
