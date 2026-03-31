/**
 * Model dictionary and mapping builders.
 */
function rebuildModelDictionary() {
  Logger.log('Rebuilding model dictionary (Ozon-only)...');
  const ozonRows = readSheetData_(APP_CONFIG.SHEETS.OZON_RAW, HEADERS.OZON_RAW.length);

  const map = {};

  ozonRows.forEach(function (r) {
    const model = normalizeText_(r[8]), color = normalizeText_(r[9]), size = normalizeText_(r[10]);
    if (!isReliableModelValue_(model)) return;
    const key = buildKey_(model, color, size);
    if (!key || key === '||') return;
    map[key] = map[key] || createMappingRow_(model, color, size);
    map[key][4] = map[key][4] || r[4];
    map[key][7] = map[key][7] || r[6];
  });

  const incoming = Object.keys(map).sort().map(function (k) {
    return map[k];
  });

  upsertRowsByKey_(APP_CONFIG.SHEETS.MAPPING, HEADERS.MAPPING, 'unified_article', incoming);
  syncModelSettings_();
  Logger.log('Model dictionary rebuilt.');
}

function createMappingRow_(model, color, size) {
  const unified = [normalizeText_(model), normalizeText_(color), normalizeText_(size)].join('-');
  return [unified, model, color, size, '', '', '', '', '', 'Да', ''];
}

function syncModelSettings_() {
  Logger.log('Syncing model settings...');
  const mapping = readSheetData_(APP_CONFIG.SHEETS.MAPPING, HEADERS.MAPPING.length);
  const modelSet = {};
  mapping.forEach(function (r) {
    const model = normalizeText_(r[1]);
    if (isReliableModelValue_(model)) modelSet[model] = true;
  });

  const sh = ensureHeaders_(APP_CONFIG.SHEETS.MODEL_SETTINGS, HEADERS.MODEL_SETTINGS);
  const existing = readSheetData_(APP_CONFIG.SHEETS.MODEL_SETTINGS, HEADERS.MODEL_SETTINGS.length);
  const existingMap = {};
  existing.forEach(function (r) {
    existingMap[normalizeText_(r[0])] = true;
  });

  const newRows = Object.keys(modelSet).sort().filter(function (model) {
    return !existingMap[model];
  }).map(function (model) {
    return [
      model,
      APP_CONFIG.DEFAULTS.COUNT_FLAG,
      APP_CONFIG.DEFAULTS.TREND,
      APP_CONFIG.DEFAULTS.STOCK_DAYS,
      APP_CONFIG.DEFAULTS.SALES_PERIOD,
      '',
      ''
    ];
  });

  if (newRows.length) {
    sh.getRange(sh.getLastRow() + 1, 1, newRows.length, HEADERS.MODEL_SETTINGS.length).setValues(newRows);
  }

  Logger.log('Model settings synced. New models: %s', newRows.length);
}

function readSheetData_(sheetName, width) {
  const sh = getOrCreateSheet_(sheetName);
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];
  return sh.getRange(2, 1, lastRow - 1, width || sh.getLastColumn()).getValues();
}
