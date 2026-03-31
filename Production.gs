/**
 * Build visible output sheets.
 */
function refreshProductionPlan() {
  Logger.log('Refreshing production plan...');
  const unified = readSheetData_(APP_CONFIG.SHEETS.UNIFIED, HEADERS.UNIFIED.length);

  const planRows = unified.map(function (r) {
    return [
      r[0], // Модель
      r[1], // Цвет
      r[2], // Размер
      r[3], // Заказы/день
      r[4], // Тренд
      r[5], // Скорость
      r[6], // Дней запаса
      r[14], // Общий остаток
      r[15], // Дней факт
      r[16], // К пошиву
      r[17], // Приоритет
      r[8], // Ozon
      r[9], // WB
      r[10], // Цеховая
      r[11], // Склад
      r[12], // В пути
      r[13], // В производстве
      r[18] // Комментарий
    ];
  });

  planRows.sort(function (a, b) {
    const aDays = a[8] === '' ? Number.POSITIVE_INFINITY : toNumber_(a[8]);
    const bDays = b[8] === '' ? Number.POSITIVE_INFINITY : toNumber_(b[8]);
    if (aDays !== bDays) return aDays - bDays;
    return b[9] - a[9];
  });

  writeDataRows_(APP_CONFIG.SHEETS.PROD_PLAN, 7, HEADERS.PROD_PLAN, planRows);
  refreshActiveModelsSheet_();
  Logger.log('Production plan refreshed. Rows: %s', planRows.length);
}

function refreshActiveModelsSheet_() {
  const unified = readSheetData_(APP_CONFIG.SHEETS.UNIFIED, HEADERS.UNIFIED.length);
  const activeRows = unified.map(function (r) {
    return [
      r[0], // Модель
      r[1], // Цвет
      r[2], // Размер
      r[3], // Заказы/день
      r[4], // Тренд
      r[5], // Скорость
      r[6], // Дней запаса
      r[14], // Общий остаток
      r[15], // Дней факт
      r[16], // К пошиву
      r[17] // Приоритет
    ];
  });
  writeDataRows_(APP_CONFIG.SHEETS.ACTIVE_MODELS, 5, HEADERS.ACTIVE_MODELS, activeRows);
}
