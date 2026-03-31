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
  const stockPayload = {
    limit: 1000,
    offset: 0,
    warehouse_type: 'ALL'
  };
  const json = ozonPost_(clientId, apiKey, '/v2/analytics/stock_on_warehouses', stockPayload);
  const items = (((json || {}).result || {}).rows) || [];
  const skuMap = fetchOzonProductInfoMap_(clientId, apiKey, items);

  const rows = items.map(function (item) {
    const sku = normalizeText_(pickFirst_(item, ['sku', 'sku_id', 'skuId', 'product_sku']) || '');
    const offerId = normalizeText_(pickFirst_(item, ['offer_id', 'offerId', 'vendor_code', 'vendorCode', 'article', 'item_code']) || '');
    const productInfo = skuMap[sku] || skuMap[offerId] || {};
    const sales30 = toNumber_(item.sales_30d || item.sales30 || 0);
    const sales90 = toNumber_(item.sales_90d || item.sales90 || 0);
    const avgOrdersPerDay = sales90 > 0 ? sales90 / 90 : (sales30 > 0 ? sales30 / 30 : 0);

    const parsed = extractOzonModelColorSize_(item, productInfo);
    const vendorCode = normalizeText_(
      pickFirst_(productInfo, ['offer_id', 'offerId', 'vendor_code', 'vendorCode', 'article']) ||
      pickFirst_(item, ['offer_id', 'offerId', 'vendor_code', 'vendorCode', 'article', 'item_code']) ||
      ''
    );
    const barcode = normalizeText_(pickFirst_(productInfo, ['barcode']) || extractBarcode_(item) || extractBarcode_(productInfo));
    const productName = normalizeText_(
      pickFirst_(productInfo, ['name', 'product_name', 'title']) ||
      pickFirst_(item, ['name', 'product_name', 'title', 'item_name']) ||
      ''
    );

    return [
      timestamp,
      normalizeText_(item.account || item.cabinet || item.seller_name || ''),
      normalizeText_(item.cluster || item.cluster_name || ''),
      normalizeText_(item.warehouse_name || item.warehouse || item.warehouse_cluster || ''),
      vendorCode,
      sku,
      barcode,
      productName,
      parsed.model,
      parsed.color,
      parsed.size,
      normalizeText_(pickFirst_(productInfo, ['category_name', 'category']) || item.category || ''),
      toNumber_(item.stock || item.quantity || 0),
      toNumber_(item.available || item.free_to_sell_amount || 0),
      toNumber_(item.in_transit || item.reserved || 0),
      sales30,
      sales90,
      avgOrdersPerDay,
      toNumber_(item.returns || 0),
      'Ozon API'
    ];
  });

  logOzonRawFillRate_(rows);
  return rows;
}

function extractOzonModelColorSize_(item, productInfo) {
  const size = normalizeText_(
    pickFirst_(productInfo, ['size', 'size_name']) ||
    findAttributeValue_(productInfo, ['размер', 'size']) ||
    item.size || item.size_name || ''
  );
  const color = normalizeText_(
    pickFirst_(productInfo, ['color', 'color_name']) ||
    findAttributeValue_(productInfo, ['цвет', 'color']) ||
    item.color || item.color_name || ''
  );
  const modelCandidates = [
    findAttributeValue_(productInfo, ['модель', 'model']),
    pickFirst_(productInfo, ['model', 'model_name', 'product_model']),
    item.model,
    item.model_name,
    item.product_model
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

function fetchOzonProductInfoMap_(clientId, apiKey, stockItems) {
  const skuSet = {};
  const offerSet = {};
  stockItems.forEach(function (item) {
    const sku = normalizeText_(pickFirst_(item, ['sku', 'sku_id', 'skuId', 'product_sku']) || '');
    const offerId = normalizeText_(pickFirst_(item, ['offer_id', 'offerId', 'vendor_code', 'vendorCode', 'article', 'item_code']) || '');
    if (sku) skuSet[sku] = true;
    if (offerId) offerSet[offerId] = true;
  });

  const skuList = Object.keys(skuSet);
  const offerList = Object.keys(offerSet);
  const map = {};

  chunkArray_(skuList, 1000).forEach(function (chunk) {
    const rows = fetchOzonProductInfoBatch_(clientId, apiKey, { sku: chunk });
    rows.forEach(function (row) {
      const keySku = normalizeText_(pickFirst_(row, ['sku', 'sku_id', 'skuId']) || '');
      const keyOffer = normalizeText_(pickFirst_(row, ['offer_id', 'offerId']) || '');
      if (keySku) map[keySku] = row;
      if (keyOffer && !map[keyOffer]) map[keyOffer] = row;
    });
  });

  if (offerList.length) {
    chunkArray_(offerList, 1000).forEach(function (chunk) {
      const rows = fetchOzonProductInfoBatch_(clientId, apiKey, { offer_id: chunk });
      rows.forEach(function (row) {
        const keySku = normalizeText_(pickFirst_(row, ['sku', 'sku_id', 'skuId']) || '');
        const keyOffer = normalizeText_(pickFirst_(row, ['offer_id', 'offerId']) || '');
        if (keySku) map[keySku] = row;
        if (keyOffer && !map[keyOffer]) map[keyOffer] = row;
      });
    });
  }

  return map;
}

function fetchOzonProductInfoBatch_(clientId, apiKey, payload) {
  const endpoints = ['/v3/product/info/list', '/v2/product/info/list'];
  for (var i = 0; i < endpoints.length; i++) {
    try {
      const json = ozonPost_(clientId, apiKey, endpoints[i], payload);
      const items = (((json || {}).result || {}).items) || ((json || {}).result) || [];
      if (items && items.length) return items;
    } catch (e) {
      Logger.log('Ozon product info endpoint failed %s: %s', endpoints[i], e.message);
    }
  }
  return [];
}

function ozonPost_(clientId, apiKey, path, payload) {
  const url = 'https://api-seller.ozon.ru' + path;
  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'Client-Id': clientId,
      'Api-Key': apiKey
    },
    payload: JSON.stringify(payload || {}),
    muteHttpExceptions: true
  };
  const response = UrlFetchApp.fetch(url, options);
  const code = response.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error('Ozon API error on ' + path + '. HTTP ' + code + ': ' + response.getContentText());
  }
  return safeJsonParse_(response.getContentText(), {});
}

function pickFirst_(obj, keys) {
  if (!obj) return '';
  for (var i = 0; i < keys.length; i++) {
    var value = obj[keys[i]];
    if (value !== null && typeof value !== 'undefined' && String(value).trim() !== '') return value;
  }
  return '';
}

function extractBarcode_(obj) {
  if (!obj) return '';
  const direct = pickFirst_(obj, ['barcode', 'bar_code']);
  if (direct) return direct;
  const barcodes = obj.barcodes || obj.barcode_list;
  if (Array.isArray(barcodes) && barcodes.length) return barcodes[0];
  return '';
}

function findAttributeValue_(productInfo, names) {
  const attrs = productInfo && (productInfo.attributes || productInfo.complex_attributes || []);
  if (!Array.isArray(attrs)) return '';
  const normalizedNames = names.map(function (n) { return normalizeText_(n).toLowerCase(); });

  for (var i = 0; i < attrs.length; i++) {
    var a = attrs[i] || {};
    var attrName = normalizeText_(a.name || a.attribute_name).toLowerCase();
    if (!attrName) continue;
    var matched = normalizedNames.some(function (n) { return attrName.indexOf(n) !== -1; });
    if (!matched) continue;
    if (Array.isArray(a.values) && a.values.length) {
      var v = a.values[0];
      var val = normalizeText_(v.value || v.name || v);
      if (val) return val;
    }
    var scalar = normalizeText_(a.value || a.text_value || '');
    if (scalar) return scalar;
  }
  return '';
}

function chunkArray_(arr, size) {
  const out = [];
  for (var i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

function logOzonRawFillRate_(rows) {
  const stats = {
    vendorCode: 0,
    barcode: 0,
    productName: 0,
    model: 0,
    color: 0,
    size: 0
  };
  rows.forEach(function (r) {
    if (normalizeText_(r[4])) stats.vendorCode++;
    if (normalizeText_(r[6])) stats.barcode++;
    if (normalizeText_(r[7])) stats.productName++;
    if (normalizeText_(r[8])) stats.model++;
    if (normalizeText_(r[9])) stats.color++;
    if (normalizeText_(r[10])) stats.size++;
  });

  Logger.log(
    'Ozon raw fill-rate: total=%s vendor=%s barcode=%s name=%s model=%s color=%s size=%s',
    rows.length,
    stats.vendorCode,
    stats.barcode,
    stats.productName,
    stats.model,
    stats.color,
    stats.size
  );
}
