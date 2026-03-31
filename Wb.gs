/**
 * Wildberries API integration.
 */
function refreshWbRaw() {
  Logger.log('Starting WB refresh...');
  const token = getScriptProperty_(APP_CONFIG.PROPERTIES.WB_API_TOKEN);
  if (!token) {
    throw new Error('WB token is not configured in Script Properties.');
  }

  const rows = fetchWbData_(token);
  fullRefreshSheet_(APP_CONFIG.SHEETS.WB_RAW, HEADERS.WB_RAW, rows);
  Logger.log('WB refresh done.');
}

function fetchWbData_(token) {
  const timestamp = nowIso_();

  // Example endpoint; adjust to your actual WB API method.
  const url = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom=2017-03-25T00:00:00';
  const options = {
    method: 'get',
    headers: {
      Authorization: token
    },
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const code = response.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error('WB API error. HTTP ' + code + ': ' + response.getContentText());
  }

  const items = safeJsonParse_(response.getContentText(), []);

  return items.map(function (item) {
    const sales30 = toNumber_(item.sales30 || item.realization30 || 0);
    const sales90 = toNumber_(item.sales90 || item.realization90 || 0);
    const avgOrdersPerDay = sales90 > 0 ? sales90 / 90 : (sales30 > 0 ? sales30 / 30 : 0);

    const parsed = extractWbModelColorSize_(item);
    return [
      timestamp,
      normalizeText_(item.supplierArticle || item.account || ''),
      normalizeText_(item.warehouseName || ''),
      normalizeText_(item.supplierArticle || ''),
      normalizeText_(item.nmId || item.sku || ''),
      normalizeText_(item.barcode || ''),
      normalizeText_(item.subject || item.name || ''),
      parsed.model,
      parsed.color,
      parsed.size,
      normalizeText_(item.category || item.subject || ''),
      toNumber_(item.quantity || item.stock || 0),
      toNumber_(item.quantityFull || item.available || 0),
      toNumber_(item.inWayToClient || 0) + toNumber_(item.inWayFromClient || 0),
      sales30,
      sales90,
      avgOrdersPerDay,
      toNumber_(item.returns || 0),
      'WB API'
    ];
  });
}

function extractWbModelColorSize_(item) {
  const size = normalizeText_(item.techSize || item.size || '');
  const color = normalizeText_(item.color || item.colorName || '');
  const modelCandidates = [
    item.model,
    item.modelName,
    item.imtName
  ];

  let model = '';
  for (let i = 0; i < modelCandidates.length; i++) {
    const candidate = normalizeText_(modelCandidates[i]);
    if (!candidate) continue;
    if (!isReliableModelValue_(candidate)) continue;
    if (size && normalizeText_(candidate).toLowerCase() === size.toLowerCase()) continue;
    model = candidate;
    break;
  }

  return {
    model: model,
    color: color,
    size: size
  };
}
