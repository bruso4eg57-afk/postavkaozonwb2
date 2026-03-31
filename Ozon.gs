/**
 * Ozon API integration.
 */
function refreshOzonRaw() {
  Logger.log('Starting Ozon refresh...');
  const clientId = getScriptProperty_(APP_CONFIG.PROPERTIES.OZON_CLIENT_ID);
  const apiKey = getScriptProperty_(APP_CONFIG.PROPERTIES.OZON_API_KEY);

  if (!clientId || !apiKey) {
    throw new Error('Ozon credentials are not configured in Script Properties.');
  }

  const rows = fetchOzonData_(clientId, apiKey);
  fullRefreshSheet_(APP_CONFIG.SHEETS.OZON_RAW, HEADERS.OZON_RAW, rows);
  Logger.log('Ozon refresh done.');
}

function fetchOzonData_(clientId, apiKey) {
  const timestamp = nowIso_();

  // Example endpoint; adjust to your actual Ozon API method.
  const url = 'https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses';
  const payload = {
    limit: 1000,
    offset: 0,
    warehouse_type: 'ALL'
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'Client-Id': clientId,
      'Api-Key': apiKey
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const code = response.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error('Ozon API error. HTTP ' + code + ': ' + response.getContentText());
  }

  const json = safeJsonParse_(response.getContentText(), {});
  const items = (((json || {}).result || {}).rows) || [];

  return items.map(function (item) {
    const sales30 = toNumber_(item.sales_30d || item.sales30 || 0);
    const sales90 = toNumber_(item.sales_90d || item.sales90 || 0);
    const avgOrdersPerDay = sales90 > 0 ? sales90 / 90 : (sales30 > 0 ? sales30 / 30 : 0);

    return [
      timestamp,
      normalizeText_(item.account || item.cabinet || ''),
      normalizeText_(item.cluster || ''),
      normalizeText_(item.warehouse_name || item.warehouse || ''),
      normalizeText_(item.offer_id || item.article || ''),
      normalizeText_(item.sku || ''),
      normalizeText_(item.barcode || ''),
      normalizeText_(item.name || item.product_name || ''),
      normalizeText_(item.model || ''),
      normalizeText_(item.color || ''),
      normalizeText_(item.size || ''),
      normalizeText_(item.category || ''),
      toNumber_(item.stock || item.quantity || 0),
      toNumber_(item.available || item.free_to_sell_amount || 0),
      toNumber_(item.in_transit || 0),
      sales30,
      sales90,
      avgOrdersPerDay,
      toNumber_(item.returns || 0),
      'Ozon API'
    ];
  });
}
