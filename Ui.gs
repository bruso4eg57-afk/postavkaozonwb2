/**
 * Spreadsheet UI menu.
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Производство API')
    .addItem('Обновить Ozon', 'menuRefreshOzon')
    .addItem('Обновить WB', 'menuRefreshWb')
    .addItem('Обновить 1C', 'menuRefreshOneC')
    .addSeparator()
    .addItem('Обновить всё', 'menuRefreshAll')
    .addSeparator()
    .addItem('Пересобрать справочник моделей', 'menuRebuildMapping')
    .addItem('Пересобрать расчет', 'menuRebuildCalculation')
    .addItem('Обновить производственный план', 'menuRefreshPlan')
    .addToUi();
}

function menuRefreshOzon() {
  refreshOzonRaw();
  rebuildModelDictionary();
  rebuildUnifiedCalculation();
  refreshProductionPlan();
}

function menuRefreshWb() {
  refreshWbRaw();
  rebuildModelDictionary();
  rebuildUnifiedCalculation();
  refreshProductionPlan();
}

function menuRefreshOneC() {
  refreshOneCRaw();
  rebuildModelDictionary();
  rebuildUnifiedCalculation();
  refreshProductionPlan();
}

function menuRefreshAll() {
  refreshAll();
}

function menuRebuildMapping() {
  rebuildModelDictionary();
}

function menuRebuildCalculation() {
  rebuildUnifiedCalculation();
}

function menuRefreshPlan() {
  refreshProductionPlan();
}
