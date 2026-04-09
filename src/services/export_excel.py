from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
import time
import zipfile
from xml.sax.saxutils import escape

MAIN_SHEET = "Приоритетность пошива"
REQUIRED_SHEETS = [
    MAIN_SHEET,
    "Сводка",
    "Проверки",
    "Неразобранные SKU",
    "RAW_1C",
    "RAW_WB",
    "RAW_Ozon",
    "Настройки",
    "Лог",
]


def _rows_to_matrix(rows: list[dict[str, Any]]) -> list[list[Any]]:
    if not rows:
        return [["Нет данных"]]
    cols = list(dict.fromkeys(k for row in rows for k in row.keys()))
    matrix: list[list[Any]] = [cols]
    for row in rows:
        matrix.append([row.get(c, "") for c in cols])
    return matrix


def _sheet_xml(matrix: list[list[Any]]) -> str:
    parts = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>', '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>']
    for r_idx, row in enumerate(matrix, start=1):
        parts.append(f'<row r="{r_idx}">')
        for c_idx, val in enumerate(row, start=1):
            col = ""
            x = c_idx
            while x:
                x, rem = divmod(x - 1, 26)
                col = chr(65 + rem) + col
            ref = f"{col}{r_idx}"
            sval = escape(str(val))
            parts.append(f'<c r="{ref}" t="inlineStr"><is><t>{sval}</t></is></c>')
        parts.append("</row>")
    parts.append("</sheetData></worksheet>")
    return "".join(parts)


def _write_xlsx_zip(path: Path, sheets: dict[str, list[list[Any]]]) -> None:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>' + ''.join(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' for i in range(1, len(sheets)+1)) + '</Types>')
        zf.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        zf.writestr("xl/workbook.xml", '<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>' + ''.join(f'<sheet name="{escape(name)}" sheetId="{i}" r:id="rId{i}"/>' for i, name in enumerate(sheets.keys(), start=1)) + '</sheets></workbook>')
        zf.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + ''.join(f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>' for i in range(1, len(sheets)+1)) + '</Relationships>')
        for idx, name in enumerate(sheets.keys(), start=1):
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", _sheet_xml(sheets[name]))


def export_report(
    output_path: str,
    priority_df: list[dict[str, Any]],
    checks_df: list[dict[str, Any]],
    unresolved_df: list[dict[str, Any]],
    raw_1c: list[dict[str, Any]],
    raw_wb: list[dict[str, Any]],
    raw_oz: list[dict[str, Any]],
    settings_df: list[dict[str, Any]],
    log_df: list[dict[str, Any]],
    template_path: str | None = None,
) -> str:
    _ = template_path
    summary_rows = [
        {"Показатель": "Всего SKU", "Значение": len(priority_df)},
        {"Показатель": "SKU в дефиците", "Значение": sum(1 for r in priority_df if int(r.get("ПРИОРИТЕТ", 4)) <= 2)},
        {"Показатель": "Суммарно к пошиву", "Значение": sum(float(r.get("Рекомендация к пошиву", 0) or 0) for r in priority_df)},
        {"Показатель": "Последнее обновление", "Значение": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
    ]

    sheets = {
        MAIN_SHEET: _rows_to_matrix(priority_df),
        "Сводка": _rows_to_matrix(summary_rows),
        "Проверки": _rows_to_matrix(checks_df),
        "Неразобранные SKU": _rows_to_matrix(unresolved_df),
        "RAW_1C": _rows_to_matrix(raw_1c),
        "RAW_WB": _rows_to_matrix(raw_wb),
        "RAW_Ozon": _rows_to_matrix(raw_oz),
        "Настройки": _rows_to_matrix(settings_df),
        "Лог": _rows_to_matrix(log_df),
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp.xlsx")
    _write_xlsx_zip(tmp, sheets)

    for _ in range(3):
        try:
            tmp.replace(out)
            return str(out)
        except PermissionError:
            time.sleep(0.3)

    fallback = out.with_name(f"{out.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{out.suffix}")
    _write_xlsx_zip(fallback, sheets)
    try:
        tmp.unlink(missing_ok=True)
    except OSError:
        pass
    return str(fallback)
