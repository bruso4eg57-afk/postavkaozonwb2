/**
 * Build unified calculation layer.
 */
function rebuildUnifiedCalculation() {
  Logger.log('Rebuilding unified calculation...');
  const mappingRows = readSheetData_(APP_CONFIG.SHEETS.MAPPING, HEADERS.MAPPING.length);
  const settingsRows = readSheetData_(APP_CONFIG.SHEETS.MODEL_SETTINGS, HEADERS.MODEL_SETTINGS.length);
  const ozonRows = readSheetData_(APP_CONFIG.SHEETS.OZON_RAW, HEADERS.OZON_RAW.length);
  const wbRows = readSheetData_(APP_CONFIG.SHEETS.WB_RAW, HEADERS.WB_RAW.length);
  const onecRows = readSheetData_(APP_CONFIG.SHEETS.ONEC_RAW, HEADERS.ONEC_RAW.length);
  const exclusions = readSizeExclusions_();

  const settingsMap = {};
  settingsRows.forEach(function (s) {
    settingsMap[normalizeText_(s[0])] = {
      count: normalizeText_(s[1]).toLowerCase() === 'да',
      trend: toNumber_(s[2]) || APP_CONFIG.DEFAULTS.TREND,
      days: toNumber_(s[3]) || APP_CONFIG.DEFAULTS.STOCK_DAYS,
      comment: normalizeText_(s[6])
    };
  });

  const agg = {};
  function ensureAgg(model, color, size) {
    if (!isReliableModelValue_(model)) return null;
    const key = buildKey_(model, color, size);
    if (!agg[key]) {
      agg[key] = {
        model: normalizeText_(model),
        color: normalizeText_(color),
        size: normalizeText_(size),
        salesPerDay: 0,
        ozonAvail: 0,
        wbAvail: 0,
        onecShop: 0,
        onecStock: 0,
        inTransit: 0,
        inProduction: 0
      };
    }
    return agg[key];
  }

  ozonRows.forEach(function (r) {
    const a = ensureAgg(r[8], r[9], r[10]);
    if (!a) return;
    a.salesPerDay += toNumber_(r[17]);
    a.ozonAvail += toNumber_(r[13]);
    a.inTransit += toNumber_(r[14]);
  });

  wbRows.forEach(function (r) {
    const a = ensureAgg(r[7], r[8], r[9]);
    if (!a) return;
    a.salesPerDay += toNumber_(r[17]);
    a.wbAvail += toNumber_(r[12]);
    a.inTransit += toNumber_(r[13]);
  });

  onecRows.forEach(function (r) {
    const a = ensureAgg(r[4], r[5], r[6]);
    if (!a) return;
    a.onecStock += toNumber_(r[9]);
    a.inProduction += toNumber_(r[10]);
    a.onecShop += Math.max(toNumber_(r[8]) - toNumber_(r[9]), 0);
  });

  const rows = [];
  mappingRows.forEach(function (m) {
    const model = normalizeText_(m[1]);
    const color = normalizeText_(m[2]);
    const size = normalizeText_(m[3]);
    if (!isReliableModelValue_(model)) return;
    const key = buildKey_(model, color, size);
    const exceptionDecision = shouldIncludeByException_(exclusions, model, color, size);
    const setting = settingsMap[model];

    if (!setting || !setting.count) return;
    if (!exceptionDecision.include) return;

    const a = agg[key] || ensureAgg(model, color, size);
    const trend = setting.trend;
    const speed = a.salesPerDay * trend;
    const days = setting.days;
    const target = speed * days;
    const totalStock = a.ozonAvail + a.wbAvail + a.onecShop + a.onecStock + a.inTransit + a.inProduction;
    const factDays = a.salesPerDay > 0 ? totalStock / a.salesPerDay : '';
    const toSew = Math.max(target - totalStock, 0);

    rows.push([
      model,
      color,
      size,
      round2_(a.salesPerDay),
      trend,
      round2_(speed),
      days,
      round2_(target),
      round2_(a.ozonAvail),
      round2_(a.wbAvail),
      round2_(a.onecShop),
      round2_(a.onecStock),
      round2_(a.inTransit),
      round2_(a.inProduction),
      round2_(totalStock),
      factDays === '' ? '' : round2_(factDays),
      round2_(toSew),
      0,
      setting.comment
    ]);
  });

  rows.sort(function (a, b) {
    const aDays = a[15] === '' ? Number.POSITIVE_INFINITY : a[15];
    const bDays = b[15] === '' ? Number.POSITIVE_INFINITY : b[15];
    if (aDays !== bDays) return aDays - bDays;
    return b[16] - a[16];
  });

  rows.forEach(function (r, i) {
    r[17] = i + 1;
  });

  fullRefreshSheet_(APP_CONFIG.SHEETS.UNIFIED, HEADERS.UNIFIED, rows);
  Logger.log('Unified calculation rebuilt. Rows: %s', rows.length);
}

function readSizeExclusions_() {
  const sh = getOrCreateSheet_(APP_CONFIG.SHEETS.SIZE_EXCEPTIONS);
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return {};
  const rows = sh.getRange(2, 1, lastRow - 1, HEADERS.SIZE_EXCEPTIONS.length).getValues();
  const map = {};
  rows.forEach(function (r) {
    const key = buildKey_(r[0], r[1], r[2]);
    if (!key || key === '||') return;
    map[key] = {
      exclude: normalizeText_(r[3]).toLowerCase() === 'да',
      force: normalizeText_(r[4]).toLowerCase() === 'да'
    };
  });
  return map;
}

function shouldIncludeByException_(exclusions, model, color, size) {
  const key = buildKey_(model, color, size);
  const rule = exclusions[key];
  if (!rule) return { include: true };
  if (rule.force) return { include: true };
  if (rule.exclude) return { include: false };
  return { include: true };
}

function round2_(n) {
  return Math.round((toNumber_(n) + Number.EPSILON) * 100) / 100;
}
