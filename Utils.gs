/**
 * Utility helpers.
 */
function getSpreadsheet_() {
  return SpreadsheetApp.openById(APP_CONFIG.SPREADSHEET_ID);
}

function getOrCreateSheet_(name) {
  const ss = getSpreadsheet_();
  return ss.getSheetByName(name) || ss.insertSheet(name);
}

function ensureHeaders_(sheetName, headers) {
  const sh = getOrCreateSheet_(sheetName);
  const current = sh.getRange(1, 1, 1, headers.length).getValues()[0];
  const same = headers.every(function (h, i) {
    return String(current[i] || '').trim() === h;
  });
  if (!same) {
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
  return sh;
}

function clearDataRows_(sheet, minColumns) {
  const lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, Math.max(minColumns || sheet.getLastColumn(), 1)).clearContent();
  }
}

function fullRefreshSheet_(sheetName, headers, rows) {
  const sh = ensureHeaders_(sheetName, headers);
  clearDataRows_(sh, headers.length);
  if (rows && rows.length) {
    sh.getRange(2, 1, rows.length, headers.length).setValues(rows);
  }
  Logger.log('Full refresh complete for %s. Rows: %s', sheetName, rows ? rows.length : 0);
}

function clearDataRangeFromRow_(sheet, startRow, width) {
  const lastRow = sheet.getLastRow();
  if (lastRow < startRow) return;
  sheet.getRange(startRow, 1, lastRow - startRow + 1, Math.max(width || sheet.getLastColumn(), 1)).clearContent();
}

function writeDataRows_(sheetName, startRow, headers, rows) {
  const sh = getOrCreateSheet_(sheetName);
  const dataWidth = rows && rows.length ? rows[0].length : headers.length;
  const writeWidth = Math.max(headers.length, dataWidth);
  clearDataRangeFromRow_(sh, startRow, writeWidth);
  if (rows && rows.length) {
    sh.getRange(startRow, 1, rows.length, dataWidth).setValues(rows);
  }
  Logger.log('Data write complete for %s from row %s. Rows: %s', sheetName, startRow, rows ? rows.length : 0);
}

function getScriptProperty_(key) {
  return PropertiesService.getScriptProperties().getProperty(key);
}

function getScriptProperties_() {
  return PropertiesService.getScriptProperties().getProperties();
}

function toNumber_(value) {
  if (value === null || value === '' || typeof value === 'undefined') return 0;
  const num = Number(String(value).replace(',', '.'));
  return isNaN(num) ? 0 : num;
}

function nowIso_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone() || 'Etc/UTC', "yyyy-MM-dd'T'HH:mm:ss");
}

function normalizeText_(value) {
  return String(value || '').trim();
}

function buildKey_(model, color, size) {
  return [normalizeText_(model), normalizeText_(color), normalizeText_(size)].join('||').toLowerCase();
}

function isLikelySizeValue_(value) {
  const v = normalizeText_(value).toLowerCase();
  if (!v) return false;
  if (/^\d{2,3}$/.test(v)) return true; // 40, 170
  if (/^\d{2,3}\s*\/\s*\d{2,3}(-\d{2,3})?$/.test(v)) return true; // 42/170-176
  if (/^(xxs|xs|s|m|l|xl|xxl|xxxl|2xl|3xl|4xl|5xl)$/.test(v)) return true;
  if (/^\d{2,3}[a-zа-я]{0,3}$/.test(v)) return true;
  return false;
}

function isLikelyTechIdentifier_(value) {
  const v = normalizeText_(value);
  if (!v) return false;
  if (/^\d{6,}$/.test(v)) return true; // nmID / sku-like numeric
  if (/^[A-Z0-9_-]{8,}$/i.test(v) && /\d/.test(v)) return true;
  return false;
}

function isReliableModelValue_(value) {
  const v = normalizeText_(value);
  if (!v) return false;
  if (isLikelySizeValue_(v)) return false;
  if (isLikelyTechIdentifier_(v)) return false;
  return true;
}

function safeJsonParse_(text, fallback) {
  try {
    return JSON.parse(text);
  } catch (e) {
    Logger.log('JSON parse failed: %s', e);
    return fallback;
  }
}

function upsertRowsByKey_(sheetName, headers, keyColumnName, incomingRows) {
  const sh = ensureHeaders_(sheetName, headers);
  const keyIndex = headers.indexOf(keyColumnName);
  if (keyIndex < 0) throw new Error('Key column not found: ' + keyColumnName);

  const lastRow = sh.getLastRow();
  const lastCol = headers.length;
  const data = lastRow > 1 ? sh.getRange(2, 1, lastRow - 1, lastCol).getValues() : [];
  const existingMap = {};

  data.forEach(function (row, i) {
    const key = normalizeText_(row[keyIndex]);
    if (key) {
      existingMap[key] = { rowNumber: i + 2, row: row };
    }
  });

  const append = [];
  const updates = [];

  incomingRows.forEach(function (row) {
    const key = normalizeText_(row[keyIndex]);
    if (!key) return;
    const existing = existingMap[key];
    if (!existing) {
      append.push(row);
      return;
    }

    const merged = existing.row.slice();
    row.forEach(function (val, idx) {
      if (idx === keyIndex) return;
      if (normalizeText_(merged[idx]) === '' && normalizeText_(val) !== '') {
        merged[idx] = val;
      }
    });
    updates.push({ rowNumber: existing.rowNumber, row: merged });
  });

  updates.forEach(function (u) {
    sh.getRange(u.rowNumber, 1, 1, lastCol).setValues([u.row]);
  });

  if (append.length) {
    sh.getRange(sh.getLastRow() + 1, 1, append.length, lastCol).setValues(append);
  }

  Logger.log('Upsert in %s. Updated: %s, Added: %s', sheetName, updates.length, append.length);
}
