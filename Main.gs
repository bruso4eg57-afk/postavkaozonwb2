/**
 * Main orchestrator functions.
 */
function runInitialSetup() {
  Logger.log('Running initial setup...');
  ensureHeaders_(APP_CONFIG.SHEETS.MODEL_SETTINGS, HEADERS.MODEL_SETTINGS);
  ensureHeaders_(APP_CONFIG.SHEETS.OZON_RAW, HEADERS.OZON_RAW);
  ensureHeaders_(APP_CONFIG.SHEETS.WB_RAW, HEADERS.WB_RAW);
  ensureHeaders_(APP_CONFIG.SHEETS.ONEC_RAW, HEADERS.ONEC_RAW);
  ensureHeaders_(APP_CONFIG.SHEETS.MAPPING, HEADERS.MAPPING);
  ensureHeaders_(APP_CONFIG.SHEETS.UNIFIED, HEADERS.UNIFIED);
  Logger.log('Initial setup done.');
}

function refreshAll() {
  Logger.log('Starting full refresh pipeline...');
  try {
    refreshOzonRaw();
  } catch (e) {
    Logger.log('Ozon refresh failed: %s', e.message);
  }

  try {
    refreshWbRaw();
  } catch (e) {
    Logger.log('WB refresh failed: %s', e.message);
  }

  try {
    refreshOneCRaw();
  } catch (e) {
    Logger.log('1C refresh failed: %s', e.message);
  }

  rebuildModelDictionary();
  rebuildUnifiedCalculation();
  refreshProductionPlan();
  Logger.log('Full refresh pipeline completed.');
}

function refreshCalculationsOnly() {
  rebuildModelDictionary();
  rebuildUnifiedCalculation();
  refreshProductionPlan();
}
