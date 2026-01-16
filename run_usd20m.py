import os
import subprocess
import sys
from typing import Optional, Dict, List, Set, Tuple

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter


# =========================
# 默认参数
# =========================
THRESHOLD_USD_DEFAULT = 20_000_000
OUTPUT_SHEET_NAME = "USD_over_20M"
RATE_SHEET_NAME = "rate"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ICON_PATH = os.path.join(BASE_DIR, "app.ico")
LOGO_SRC_PATH = os.path.join(BASE_DIR, "ing-logo.png")


# =========================
# 工具函数
# =========================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """列名去空格，并做一些可能的列名兼容映射"""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    target_cols = {
        "Transfer Amount": ["Transfer Amount", "TransferAmount", "Transfer_Amount"],
        "SettleCurrency": ["SettleCurrency", "Settle Currency", "Settle_Currency"],
        "Product Type": ["Product Type", "ProductType", "Product_Type"],
        "Trade Id": ["Trade Id", "TradeId", "Trade_ID", "TradeID"],
        "Transfer Type": ["Transfer Type", "TransferType", "Transfer_Type"],
    }

    col_map_insensitive = {c.lower().replace(" ", "").replace("_", ""): c for c in df.columns}
    rename_dict = {}

    for std, candidates in target_cols.items():
        if std in df.columns:
            continue
        found = None
        for cand in candidates:
            key = cand.lower().replace(" ", "").replace("_", "")
            if key in col_map_insensitive:
                found = col_map_insensitive[key]
                break
        if found:
            rename_dict[found] = std

    if rename_dict:
        df = df.rename(columns=rename_dict)

    return df


def parse_amount_to_float(series: pd.Series) -> pd.Series:
    """金额解析：去逗号/空格，转数值（用于计算）"""
    s = series.astype(str).str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def apply_tradeid_rule_keep_all_none(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """
    只对 Product Type 为 Cash / StructuredFlows 的记录执行：
    - 同 Trade Id：若存在 Transfer Type == NONE，则保留该 Trade Id 下所有 NONE 行
    - 若不存在 NONE，则只保留该 Trade Id 下第一行（按原顺序）
    其他 Product Type：不变
    """
    df = df_filtered.copy()

    required = ["Product Type", "Trade Id", "Transfer Type"]
    if any(c not in df.columns for c in required):
        return df

    df["__row_order"] = range(len(df))

    mask = df["Product Type"].astype(str).str.strip().isin(["Cash", "StructuredFlows"])
    df_need = df[mask].copy()
    df_keep = df[~mask].copy()

    if df_need.empty:
        return df_keep.drop(columns=["__row_order"], errors="ignore")

    df_need["Trade Id"] = df_need["Trade Id"].astype(str)

    kept_parts = []
    for _, g in df_need.groupby("Trade Id", dropna=False, sort=False):
        g_sorted = g.sort_values("__row_order")
        g_none = g_sorted[g_sorted["Transfer Type"].astype(str).str.strip().str.upper() == "NONE"]
        if not g_none.empty:
            kept_parts.append(g_none)
        else:
            kept_parts.append(g_sorted.iloc[[0]])

    picked = pd.concat(kept_parts, ignore_index=False)

    out = pd.concat([df_keep, picked], ignore_index=True)
    out = out.sort_values("__row_order").drop(columns=["__row_order"], errors="ignore").reset_index(drop=True)
    return out


def autofit_column_width(ws, min_width=8, max_width=60, extra=2):
    """根据单元格内容长度自适应列宽"""
    col_max = {}
    for row in ws.iter_rows(values_only=True):
        for j, val in enumerate(row, start=1):
            length = 0 if val is None else len(str(val))
            col_max[j] = max(col_max.get(j, 0), length)

    for j, max_len in col_max.items():
        width = max(min_width, min(max_width, max_len + extra))
        ws.column_dimensions[get_column_letter(j)].width = width


def write_df_to_existing_workbook(file_path: str, sheet_name: str, df: pd.DataFrame):
    """写入到同一个 Excel 的最后一个 sheet（若同名则覆盖）"""
    wb = load_workbook(file_path)

    if sheet_name in wb.sheetnames:
        wb.remove(wb[sheet_name])

    ws = wb.create_sheet(title=sheet_name)

    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    ws.freeze_panes = "A2"
    autofit_column_width(ws)

    wb.save(file_path)


def open_file_path(file_path: str) -> None:
    if not file_path or not os.path.exists(file_path):
        return
    try:
        if sys.platform.startswith("win"):
            os.startfile(file_path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.run(["open", file_path], check=False)
        else:
            subprocess.run(["xdg-open", file_path], check=False)
    except Exception:
        pass


# =========================
# rate 读取：支持 LIMIT AMOUNT 是公式（优先用 EXCHANGE VS CNY 推导）
# =========================
def _u(x) -> str:
    return str(x).strip().upper()


def _num(x) -> Optional[float]:
    v = pd.to_numeric(str(x).replace(",", "").replace(" ", ""), errors="coerce")
    if pd.isna(v):
        return None
    v = float(v)
    if v <= 0:
        return None
    return v


def _find_rate_sheet(file_path: str) -> str:
    xls = pd.ExcelFile(file_path, engine="openpyxl")
    for name in xls.sheet_names:
        if str(name).strip().lower() == RATE_SHEET_NAME:
            return name

    # fallback：扫描任意 sheet 前 40 行，出现 CCY 则认为是 rate 表
    for name in xls.sheet_names:
        try:
            head = pd.read_excel(file_path, sheet_name=name, header=None, engine="openpyxl", nrows=40)
        except Exception:
            continue
        for _, row in head.iterrows():
            cells = [_u(x) for x in row.tolist()]
            if "CCY" in cells:
                return name

    raise ValueError("未找到 rate sheet（无法识别含 CCY 表头的表）。")


def _find_threshold_usd(raw: pd.DataFrame, default_threshold: float) -> float:
    """
    在 raw 里找包含 USD 的行，从该行取最大的“明显是金额”的数字（>1000），避免误取 USD=1。
    """
    best = None
    for _, row in raw.iterrows():
        cells = [_u(x) for x in row.tolist()]
        if "USD" not in cells:
            continue
        nums = []
        for x in row.tolist():
            v = _num(x)
            if v is not None and v > 1000:
                nums.append(v)
        if nums:
            cand = max(nums)
            best = cand if best is None else max(best, cand)

    return float(best) if best is not None else float(default_threshold)


def _detect_header_cols(raw: pd.DataFrame) -> Tuple[int, int, Optional[int], Optional[int]]:
    """
    返回：header_row_idx, ccy_col_idx, exchange_col_idx, limit_col_idx
    支持 LIMIT / AMOUNT 分列、或一个单元格写 LIMIT AMOUNT。
    """
    for i, row in raw.iterrows():
        cells = [_u(x) for x in row.tolist()]
        if "CCY" not in cells:
            continue

        ccy_col = cells.index("CCY")

        exch_col = None
        for j, c in enumerate(cells):
            if "EXCHANGE" in c and "RATE" in c:
                exch_col = j
                break

        limit_col = None
        for j, c in enumerate(cells):
            if "LIMIT" in c and "AMOUNT" in c:
                limit_col = j
                break
        if limit_col is None:
            for j, c in enumerate(cells):
                if c == "LIMIT" and j + 1 < len(cells) and cells[j + 1] == "AMOUNT":
                    limit_col = j
                    break
        if limit_col is None:
            for j, c in enumerate(cells):
                if "LIMIT" in c:
                    limit_col = j
                    break

        return int(i), int(ccy_col), exch_col, limit_col

    raise ValueError("rate 表格式无法识别：未找到 CCY 表头行。")


def load_rates_and_threshold(file_path: str) -> Tuple[Dict[str, float], float, str, str]:
    """
    返回：rates(外币/美元), threshold_usd, rate_sheet_name, source
    source: exchange_vs_cny / limit_amount
    """
    rate_sheet = _find_rate_sheet(file_path)
    raw = pd.read_excel(file_path, sheet_name=rate_sheet, header=None, engine="openpyxl")

    threshold_usd = _find_threshold_usd(raw, THRESHOLD_USD_DEFAULT)
    header_row, ccy_col, exch_col, limit_col = _detect_header_cols(raw)

    exch_map: Dict[str, float] = {}
    limit_map: Dict[str, float] = {}

    for _, r in raw.iloc[header_row + 1 :].iterrows():
        ccy = _u(r.iloc[ccy_col])
        if not ccy or ccy in ("CCY", "NAN") or len(ccy) != 3:
            continue

        if exch_col is not None:
            v = _num(r.iloc[exch_col])
            if v is not None:
                exch_map[ccy] = float(v)

        if limit_col is not None:
            v2 = _num(r.iloc[limit_col])
            if v2 is not None:
                limit_map[ccy] = float(v2)

    rates: Dict[str, float] = {}

    # 1) 优先用 EXCHANGE VS CNY 推导（不依赖 LIMIT AMOUNT 公式结果）
    cny_per_usd = exch_map.get("CNY")
    if exch_map and cny_per_usd and cny_per_usd > 0:
        for ccy, cny_per_ccy in exch_map.items():
            if ccy == "USD":
                rates["USD"] = 1.0
                continue
            if cny_per_ccy <= 0:
                continue
            # CCY per USD = (CNY per USD) / (CNY per CCY)
            rates[ccy] = float(cny_per_usd) / float(cny_per_ccy)
        rates.setdefault("USD", 1.0)
        return rates, float(threshold_usd), rate_sheet, "exchange_vs_cny"

    # 2) fallback：LIMIT AMOUNT / threshold
    if not limit_map:
        raise ValueError("rate 表无法读取有效汇率：EXCHANGE 推导失败，且 LIMIT AMOUNT 无有效数值。")
    for ccy, amt in limit_map.items():
        rates[ccy] = float(amt) / float(threshold_usd)
    rates.setdefault("USD", 1.0)
    return rates, float(threshold_usd), rate_sheet, "limit_amount"


def prescan_missing_currencies(file_path: str, rate_sheet_name: str, rates: Dict[str, float]) -> List[str]:
    """预扫描前 3 个数据表，找缺失 SettleCurrency 对应的汇率"""
    xls = pd.ExcelFile(file_path, engine="openpyxl")
    data_sheets = [s for s in xls.sheet_names if s not in (rate_sheet_name, OUTPUT_SHEET_NAME)]
    sheet_names = data_sheets[:3]

    missing: Set[str] = set()
    for sh in sheet_names:
        try:
            df = pd.read_excel(file_path, sheet_name=sh, engine="openpyxl")
        except Exception:
            continue
        df = normalize_columns(df)
        if "SettleCurrency" not in df.columns:
            continue

        ccy_upper = df["SettleCurrency"].astype(str).str.upper().str.strip()
        for c in ccy_upper.unique().tolist():
            c = str(c).strip().upper()
            if not c or c == "NAN" or len(c) != 3:
                continue
            if c not in rates:
                missing.add(c)

    return sorted(missing)


# =========================
# 主处理逻辑
# =========================
def process_workbook(
    file_path: str,
    rates: Dict[str, float],
    threshold_usd: float,
    rate_sheet_name: str,
    progress_cb=None,
    status_cb=None,
) -> Tuple[int, Set[str]]:
    """
    返回：写入行数、处理过程中发现的缺失币种集合（用于最终提示）
    """
    def status(msg: str):
        if status_cb:
            status_cb(msg)

    def progress(p: int):
        if progress_cb:
            progress_cb(int(max(0, min(100, p))))

    xls = pd.ExcelFile(file_path, engine="openpyxl")
    data_sheets = [s for s in xls.sheet_names if s not in (rate_sheet_name, OUTPUT_SHEET_NAME)]
    sheet_names = data_sheets[:3]

    if not sheet_names:
        write_df_to_existing_workbook(file_path, OUTPUT_SHEET_NAME, pd.DataFrame())
        status("未找到可处理的数据表，已生成空结果。")
        progress(100)
        return 0, set()

    results = []
    missing_ccy_set: Set[str] = set()

    total_steps = len(sheet_names) + 1
    progress(0)

    for i, sh in enumerate(sheet_names, start=1):
        status(f"处理中：{sh}")
        df = pd.read_excel(file_path, sheet_name=sh, engine="openpyxl")
        df = normalize_columns(df)

        must_cols = ["Transfer Amount", "SettleCurrency", "Product Type", "Trade Id", "Transfer Type"]
        miss = [c for c in must_cols if c not in df.columns]
        if miss:
            progress(int(i / total_steps * 100))
            continue

        amt_num = parse_amount_to_float(df["Transfer Amount"])
        abs_amt = amt_num.abs()

        ccy_upper = df["SettleCurrency"].astype(str).str.upper().str.strip()
        rate_series = ccy_upper.map(rates)

        missing_ccy = sorted(set(ccy_upper[rate_series.isna()].tolist()))
        missing_ccy = [c for c in missing_ccy if c and c != "NAN"]
        if missing_ccy:
            missing_ccy_set.update(missing_ccy)

        usd_amt = abs_amt / rate_series
        df_big = df[usd_amt > threshold_usd].copy()

        if not df_big.empty:
            df_big = apply_tradeid_rule_keep_all_none(df_big)
            df_big.loc[:, "Transfer Amount"] = abs_amt.loc[df_big.index]
            results.append(df_big)

        progress(int(i / total_steps * 100))

    status("写入结果...")
    if results:
        out_df = pd.concat(results, ignore_index=True)
        write_df_to_existing_workbook(file_path, OUTPUT_SHEET_NAME, out_df)
        progress(100)
        return len(out_df), missing_ccy_set

    write_df_to_existing_workbook(file_path, OUTPUT_SHEET_NAME, pd.DataFrame())
    progress(100)
    return 0, missing_ccy_set


# =========================
# UI
# =========================
def load_qt_modules():
    try:
        from PySide6 import QtCore, QtGui, QtWidgets
        return "PySide6", QtCore, QtGui, QtWidgets, QtCore.Signal, QtCore.Slot
    except Exception:
        pass

    try:
        from PyQt6 import QtCore, QtGui, QtWidgets
        return "PyQt6", QtCore, QtGui, QtWidgets, QtCore.pyqtSignal, QtCore.pyqtSlot
    except Exception:
        pass

    try:
        from PyQt5 import QtCore, QtGui, QtWidgets
        return "PyQt5", QtCore, QtGui, QtWidgets, QtCore.pyqtSignal, QtCore.pyqtSlot
    except Exception:
        return None


def launch_ui() -> bool:
    qt_modules = load_qt_modules()
    if not qt_modules:
        print("UI 依赖 Qt (PySide6 / PyQt6 / PyQt5)，当前环境未安装。")
        print("请先执行: python -m pip install PySide6")
        return False

    _qt_name, QtCore, QtGui, QtWidgets, Signal, Slot = qt_modules
    Qt = QtCore.Qt

    def get_qt_plugins_path() -> Optional[str]:
        qlib = getattr(QtCore, "QLibraryInfo", None)
        if qlib is None:
            return None
        lib_path = getattr(qlib, "LibraryPath", None)
        if lib_path and hasattr(qlib, "path"):
            return qlib.path(lib_path.PluginsPath)
        if hasattr(qlib, "location") and hasattr(qlib, "PluginsPath"):
            return qlib.location(qlib.PluginsPath)
        return None

    plugins_path = get_qt_plugins_path()
    if plugins_path:
        platform_path = os.path.join(plugins_path, "platforms")
        os.environ.setdefault("QT_PLUGIN_PATH", plugins_path)
        os.environ.setdefault("QT_QPA_PLATFORM_PLUGIN_PATH", platform_path)
        try:
            QtCore.QCoreApplication.addLibraryPath(plugins_path)
        except Exception:
            pass

    align_flag = getattr(Qt, "AlignmentFlag", None)
    if align_flag:
        align_left = align_flag.AlignLeft
        align_right = align_flag.AlignRight
        align_center = align_flag.AlignCenter
        align_vcenter = align_flag.AlignVCenter
    else:
        align_left = Qt.AlignLeft
        align_right = Qt.AlignRight
        align_center = Qt.AlignCenter
        align_vcenter = Qt.AlignVCenter

    no_pen = getattr(Qt, "NoPen", None)
    if no_pen is None:
        no_pen = Qt.PenStyle.NoPen

    wa_styled = getattr(Qt, "WA_StyledBackground", None)
    if wa_styled is None:
        wa_styled = Qt.WidgetAttribute.WA_StyledBackground

    smooth_transform = getattr(Qt, "SmoothTransformation", None)
    if smooth_transform is None:
        smooth_transform = Qt.TransformationMode.SmoothTransformation

    aspect_keep = getattr(Qt, "KeepAspectRatio", None)
    if aspect_keep is None:
        aspect_keep = Qt.AspectRatioMode.KeepAspectRatio

    render_hint = getattr(QtGui.QPainter, "Antialiasing", None)
    if render_hint is None:
        render_hint = QtGui.QPainter.RenderHint.Antialiasing

    def font_bold_weight():
        if hasattr(QtGui.QFont, "Weight"):
            return QtGui.QFont.Weight.Bold
        return QtGui.QFont.Bold

    def create_rounded_pixmap(path: str, size: int, radius: int):
        if not os.path.exists(path):
            return None
        pixmap = QtGui.QPixmap(path)
        if pixmap.isNull():
            return None

        scaled = pixmap.scaled(size, size, aspect_keep, smooth_transform)
        target = QtGui.QPixmap(size, size)
        target.fill(QtGui.QColor(0, 0, 0, 0))

        painter = QtGui.QPainter(target)
        painter.setRenderHint(render_hint, True)

        clip_path = QtGui.QPainterPath()
        clip_path.addRoundedRect(QtCore.QRectF(0, 0, size, size), radius, radius)
        painter.setClipPath(clip_path)

        x_offset = int((size - scaled.width()) / 2)
        y_offset = int((size - scaled.height()) / 2)
        painter.drawPixmap(x_offset, y_offset, scaled)
        painter.end()

        return target

    def is_pyqt6() -> bool:
        return _qt_name == "PyQt6"

    def make_non_editable(item):
        try:
            if is_pyqt6():
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            else:
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
        except Exception:
            pass
        return item

    DIALOG_STYLE = """
        QDialog {
            background-color: #0F212B;
            border: 1px solid #173443;
            border-radius: 14px;
        }
        QLabel { color: #E6F1F5; }
        QLabel#hint { color: #9FB2BC; }
        QTableWidget {
            background: #0A1720;
            border: 1px solid #23414D;
            border-radius: 10px;
            gridline-color: #23414D;
            color: #E6F1F5;
        }
        QHeaderView::section {
            background-color: #132833;
            color: #E6F1F5;
            padding: 6px;
            border: 1px solid #23414D;
        }
        QPushButton#primary {
            background-color: #44E6C2;
            color: #0A1B22;
            border-radius: 10px;
            padding: 8px 16px;
        }
        QPushButton#primary:hover { background-color: #3BD9B7; }
        QPushButton#secondary {
            background-color: #132833;
            color: #E6F1F5;
            border: 1px solid #23414D;
            border-radius: 10px;
            padding: 8px 16px;
        }
        QPushButton#secondary:hover { border-color: #44E6C2; }
    """

    class BackgroundWidget(QtWidgets.QWidget):
        def paintEvent(self, event):
            painter = QtGui.QPainter(self)
            gradient = QtGui.QLinearGradient(0, 0, 0, self.height())
            gradient.setColorAt(0, QtGui.QColor("#0B1F2A"))
            gradient.setColorAt(1, QtGui.QColor("#0E242E"))
            painter.fillRect(self.rect(), gradient)

            painter.setRenderHint(render_hint, True)
            painter.setPen(no_pen)
            painter.setBrush(QtGui.QColor("#123645"))
            painter.drawEllipse(-120, -80, 380, 380)

            painter.setBrush(QtGui.QColor("#123240"))
            painter.drawEllipse(self.width() - 260, self.height() - 200, 480, 480)

            pen = QtGui.QPen(QtGui.QColor("#1B3946"), 2)
            painter.setPen(pen)
            line_y = int(self.height() * 0.32)
            painter.drawLine(0, line_y, self.width(), line_y)

    class MissingRatesDialog(QtWidgets.QDialog):
        """
        缺失币种：表格填空（外币/美元）。
        - 继续应用：返回 {CCY: rate}
        - 跳过：返回 {}
        """
        def __init__(self, missing_list: List[str], parent=None):
            super().__init__(parent)
            self.setWindowTitle("补充缺失汇率")
            self.setMinimumSize(560, 380)
            self.setStyleSheet(DIALOG_STYLE)
            self._rates: Dict[str, float] = {}
            self._skipped = False

            layout = QtWidgets.QVBoxLayout(self)
            layout.setContentsMargins(16, 16, 16, 16)
            layout.setSpacing(10)

            title = QtWidgets.QLabel("补充缺失汇率")
            f = QtGui.QFont()
            f.setPointSize(13)
            f.setWeight(font_bold_weight())
            title.setFont(f)
            layout.addWidget(title)

            hint = QtWidgets.QLabel(
                "以下币种在 rate 表中无法自动推导“外币/美元”。\n"
                "请在表格中填写（例如 JPY 填 156.90）。留空表示跳过该币种。"
            )
            hint.setObjectName("hint")
            hint.setWordWrap(True)
            layout.addWidget(hint)

            self.table = QtWidgets.QTableWidget()
            self.table.setColumnCount(2)
            self.table.setHorizontalHeaderLabels(["CCY", "外币/美元"])
            self.table.setRowCount(len(missing_list))
            self.table.verticalHeader().setVisible(False)
            self.table.horizontalHeader().setStretchLastSection(True)

            for i, ccy in enumerate(missing_list):
                item_ccy = make_non_editable(QtWidgets.QTableWidgetItem(ccy))
                self.table.setItem(i, 0, item_ccy)
                self.table.setItem(i, 1, QtWidgets.QTableWidgetItem(""))

            self.table.resizeColumnsToContents()
            layout.addWidget(self.table, 1)

            btn_row = QtWidgets.QHBoxLayout()
            btn_row.addStretch(1)

            self.skip_btn = QtWidgets.QPushButton("跳过")
            self.skip_btn.setObjectName("secondary")
            self.ok_btn = QtWidgets.QPushButton("继续应用")
            self.ok_btn.setObjectName("primary")

            btn_row.addWidget(self.skip_btn)
            btn_row.addWidget(self.ok_btn)
            layout.addLayout(btn_row)

            self.skip_btn.clicked.connect(self._on_skip)
            self.ok_btn.clicked.connect(self._on_ok)

        def _on_skip(self):
            self._skipped = True
            self._rates = {}
            self.accept()

        def _on_ok(self):
            rates: Dict[str, float] = {}
            bad = []
            for r in range(self.table.rowCount()):
                ccy = (self.table.item(r, 0).text() or "").strip().upper()
                val_item = self.table.item(r, 1)
                txt = (val_item.text() if val_item else "").strip().replace(",", "").replace(" ", "")
                if not txt:
                    continue
                try:
                    v = float(txt)
                except ValueError:
                    bad.append(ccy)
                    continue
                if v <= 0:
                    bad.append(ccy)
                    continue
                rates[ccy] = float(v)

            if bad:
                QtWidgets.QMessageBox.warning(self, "提示", "以下币种输入不合法（需为正数）：\n" + ", ".join(bad))
                return

            self._rates = rates
            self.accept()

        def result_rates(self) -> Dict[str, float]:
            return dict(self._rates)

        def skipped(self) -> bool:
            return bool(self._skipped)

    class Worker(QtCore.QObject):
        progress = Signal(int)
        status = Signal(str)
        rates_ready = Signal(object)          # {"rates":..., "threshold":..., "rate_sheet":..., "source":...}
        request_missing_rates = Signal(object)  # list[str]
        finished = Signal(object)             # {"count":..., "threshold":..., "missing":..., "skipped":...}
        error = Signal(str)

        def __init__(self, file_path: str):
            super().__init__()
            self.file_path = file_path
            self._missing_reply: Dict[str, float] = {}
            self._missing_loop: Optional["QtCore.QEventLoop"] = None

        @Slot(object)
        def receive_missing_rates(self, payload: object):
            """
            payload: {"rates": {CCY: rate}}
            """
            rates = {}
            if isinstance(payload, dict):
                rates = payload.get("rates", {}) or {}
            self._missing_reply = dict(rates)
            if self._missing_loop is not None:
                self._missing_loop.quit()

        def _wait_missing_rates(self, missing_list: List[str]) -> Dict[str, float]:
            self._missing_reply = {}
            self.request_missing_rates.emit(missing_list)
            self._missing_loop = QtCore.QEventLoop()
            self._missing_loop.exec()
            self._missing_loop = None
            return dict(self._missing_reply)

        def run(self):
            try:
                self.progress.emit(0)
                self.status.emit("读取 rate 表并识别阈值/汇率...")

                rates, threshold, rate_sheet, source = load_rates_and_threshold(self.file_path)

                # 展示识别结果
                self.rates_ready.emit(
                    {
                        "rates": rates,
                        "threshold": threshold,
                        "rate_sheet": rate_sheet,
                        "source": source,
                    }
                )

                self.status.emit("检查缺失币种汇率...")
                missing_list = prescan_missing_currencies(self.file_path, rate_sheet, rates)

                skipped_ccy: Set[str] = set()
                if missing_list:
                    self.status.emit("需要补充缺失汇率...")
                    provided = self._wait_missing_rates(missing_list)
                    if provided:
                        rates.update(provided)
                    skipped_ccy = set(missing_list) - set(provided.keys())

                    # 更新展示（补充后）
                    self.rates_ready.emit(
                        {
                            "rates": rates,
                            "threshold": threshold,
                            "rate_sheet": rate_sheet,
                            "source": source,
                        }
                    )

                self.status.emit("开始筛选并导出...")
                def progress_cb(p: int):
                    self.progress.emit(p)

                def status_cb(msg: str):
                    self.status.emit(msg)

                count, missing_during = process_workbook(
                    self.file_path,
                    rates=rates,
                    threshold_usd=threshold,
                    rate_sheet_name=rate_sheet,
                    progress_cb=progress_cb,
                    status_cb=status_cb,
                )

                # 汇总缺失提示：用户跳过 + 处理中仍缺
                final_missing = set(missing_during) | set(skipped_ccy)

                self.finished.emit(
                    {
                        "count": int(count),
                        "threshold": float(threshold),
                        "output_sheet": OUTPUT_SHEET_NAME,
                        "missing": sorted(final_missing),
                        "skipped": sorted(list(skipped_ccy)),
                    }
                )

            except Exception as exc:
                self.error.emit(str(exc))

    class MainWindow(QtWidgets.QWidget):
        missing_rates_result = Signal(object)  # -> worker.receive_missing_rates

        def __init__(self):
            super().__init__()
            self._worker_thread = None
            self._worker = None
            self._build_ui()

        def _build_ui(self):
            self.setWindowTitle("USD 大额筛选")
            self.setMinimumSize(780, 540)
            self.resize(900, 600)
            if os.path.exists(ICON_PATH):
                self.setWindowIcon(QtGui.QIcon(ICON_PATH))

            base_font = QtGui.QFont("Avenir Next", 11)
            self.setFont(base_font)

            title_font = QtGui.QFont("Avenir Next", 22)
            title_font.setWeight(font_bold_weight())
            subtitle_font = QtGui.QFont("Avenir Next", 11)
            label_font = QtGui.QFont("Avenir Next", 11)
            button_font = QtGui.QFont("Avenir Next", 11)
            button_font.setWeight(font_bold_weight())
            small_font = QtGui.QFont("Avenir Next", 9)
            footer_font = QtGui.QFont("Avenir Next", 9)

            background = BackgroundWidget()
            root_layout = QtWidgets.QVBoxLayout(self)
            root_layout.setContentsMargins(0, 0, 0, 0)
            root_layout.addWidget(background)

            panel = QtWidgets.QFrame()
            panel.setObjectName("panel")
            panel.setAttribute(wa_styled, True)
            panel.setMaximumWidth(780)

            panel_layout = QtWidgets.QVBoxLayout(panel)
            panel_layout.setContentsMargins(26, 24, 26, 22)
            panel_layout.setSpacing(12)

            header_layout = QtWidgets.QHBoxLayout()
            header_layout.setSpacing(16)

            logo_pixmap = create_rounded_pixmap(LOGO_SRC_PATH, 86, 18)
            if logo_pixmap:
                logo_label = QtWidgets.QLabel()
                logo_label.setFixedSize(86, 86)
                logo_label.setPixmap(logo_pixmap)
                logo_label.setAlignment(align_center)
                header_layout.addWidget(logo_label, 0, align_vcenter)

            title_layout = QtWidgets.QVBoxLayout()
            title_label = QtWidgets.QLabel("USD 大额筛选")
            title_label.setFont(title_font)
            subtitle_label = QtWidgets.QLabel("自动识别 rate 表汇率与 USD 阈值，补充缺失后自动导出")
            subtitle_label.setObjectName("subtitle")
            subtitle_label.setFont(subtitle_font)
            subtitle_label.setWordWrap(True)
            title_layout.addWidget(title_label)
            title_layout.addWidget(subtitle_label)
            header_layout.addLayout(title_layout, 1)

            panel_layout.addLayout(header_layout)

            file_label = QtWidgets.QLabel("Excel 文件")
            file_label.setFont(label_font)
            panel_layout.addWidget(file_label)

            file_row = QtWidgets.QHBoxLayout()
            file_row.setSpacing(12)
            self.path_edit = QtWidgets.QLineEdit()
            self.path_edit.setPlaceholderText("请选择 Excel 文件")
            file_row.addWidget(self.path_edit, 1)

            self.browse_btn = QtWidgets.QPushButton("选择文件")
            self.browse_btn.setObjectName("secondaryButton")
            self.browse_btn.setFont(button_font)
            self.browse_btn.clicked.connect(self.choose_file)
            file_row.addWidget(self.browse_btn)

            panel_layout.addLayout(file_row)

            hint_label = QtWidgets.QLabel(f"输出 Sheet：{OUTPUT_SHEET_NAME}（Transfer Amount 输出为绝对值）")
            hint_label.setObjectName("hint")
            hint_label.setFont(small_font)
            panel_layout.addWidget(hint_label)

            rate_label = QtWidgets.QLabel("识别结果（阈值 & 外币/美元）")
            rate_label.setFont(label_font)
            panel_layout.addWidget(rate_label)

            self.rate_box = QtWidgets.QPlainTextEdit()
            self.rate_box.setObjectName("rateBox")
            self.rate_box.setReadOnly(True)
            self.rate_box.setMinimumHeight(140)
            self.rate_box.setMaximumHeight(180)
            self.rate_box.setFont(small_font)
            self.rate_box.setPlainText("尚未读取。")
            panel_layout.addWidget(self.rate_box)

            action_row = QtWidgets.QHBoxLayout()
            self.run_btn = QtWidgets.QPushButton("开始筛选并导出")
            self.run_btn.setObjectName("primaryButton")
            self.run_btn.setFont(button_font)
            self.run_btn.clicked.connect(self.start_task)
            action_row.addWidget(self.run_btn, 0, align_left)
            action_row.addStretch(1)
            panel_layout.addLayout(action_row)

            self.progress_bar = QtWidgets.QProgressBar()
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(0)
            self.progress_bar.setTextVisible(True)
            panel_layout.addWidget(self.progress_bar)

            self.status_label = QtWidgets.QLabel("等待选择文件…")
            self.status_label.setObjectName("status")
            self.status_label.setFont(small_font)
            panel_layout.addWidget(self.status_label)

            footer_row = QtWidgets.QHBoxLayout()
            footer_row.addStretch(1)
            footer_label = QtWidgets.QLabel("i Designed by 余智秋 in Shanghai.")
            footer_label.setObjectName("footer")
            footer_label.setFont(footer_font)
            footer_row.addWidget(footer_label, 0, align_right)
            panel_layout.addLayout(footer_row)

            background_layout = QtWidgets.QVBoxLayout(background)
            background_layout.setContentsMargins(32, 28, 32, 28)
            background_layout.addStretch(1)
            background_layout.addWidget(panel, 0, align_center)
            background_layout.addStretch(1)

            panel.setStyleSheet(
                """
                QFrame#panel {
                    background-color: #0F212B;
                    border: 1px solid #173443;
                    border-radius: 18px;
                }
                QLabel { color: #E6F1F5; }
                QLabel#subtitle, QLabel#hint, QLabel#status { color: #9FB2BC; }
                QLabel#footer { color: #D6B25E; }
                QLineEdit {
                    background: #0A1720;
                    border: 1px solid #23414D;
                    border-radius: 10px;
                    padding: 8px 10px;
                    color: #E6F1F5;
                }
                QLineEdit:focus { border-color: #44E6C2; }
                QPlainTextEdit#rateBox {
                    background: #0A1720;
                    border: 1px solid #23414D;
                    border-radius: 10px;
                    padding: 6px 8px;
                    color: #E6F1F5;
                }
                QPushButton#primaryButton {
                    background-color: #44E6C2;
                    color: #0A1B22;
                    border-radius: 10px;
                    padding: 8px 18px;
                }
                QPushButton#primaryButton:hover { background-color: #3BD9B7; }
                QPushButton#primaryButton:pressed { background-color: #2EB596; }
                QPushButton#secondaryButton {
                    background-color: #132833;
                    color: #E6F1F5;
                    border: 1px solid #23414D;
                    border-radius: 10px;
                    padding: 8px 16px;
                }
                QPushButton#secondaryButton:hover { border-color: #44E6C2; }
                QProgressBar {
                    background: #0A1720;
                    border: 1px solid #23414D;
                    border-radius: 6px;
                    text-align: center;
                    color: #9FB2BC;
                }
                QProgressBar::chunk {
                    background: #44E6C2;
                    border-radius: 6px;
                }
                """
            )

        def choose_file(self):
            file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
                self, "选择 Excel 文件", BASE_DIR, "Excel 文件 (*.xlsx *.xlsm *.xls)"
            )
            if file_path:
                self.path_edit.setText(file_path)
                self.status_label.setText("已选择文件，准备开始。")

        def _set_running(self, running: bool):
            self.run_btn.setEnabled(not running)
            self.browse_btn.setEnabled(not running)
            self.path_edit.setEnabled(not running)

        def start_task(self):
            file_path = self.path_edit.text().strip()
            if not file_path:
                QtWidgets.QMessageBox.warning(self, "提示", "请先选择 Excel 文件。")
                return
            if not os.path.exists(file_path):
                QtWidgets.QMessageBox.critical(self, "错误", "文件不存在，请重新选择。")
                return

            self.progress_bar.setValue(0)
            self.status_label.setText("准备处理中…")
            self.rate_box.setPlainText("读取 rate 中…")
            self._set_running(True)

            self._worker_thread = QtCore.QThread()
            self._worker = Worker(file_path)
            self._worker.moveToThread(self._worker_thread)

            self.missing_rates_result.connect(self._worker.receive_missing_rates)

            self._worker_thread.started.connect(self._worker.run)
            self._worker.progress.connect(self.progress_bar.setValue)
            self._worker.status.connect(self.status_label.setText)
            self._worker.rates_ready.connect(self._handle_rates_ready)
            self._worker.request_missing_rates.connect(self._prompt_missing_rates)
            self._worker.finished.connect(self._handle_finished)
            self._worker.error.connect(self._handle_error)

            self._worker.finished.connect(self._worker_thread.quit)
            self._worker.error.connect(self._worker_thread.quit)
            self._worker_thread.finished.connect(self._cleanup_worker)

            self._worker_thread.start()

        def _handle_rates_ready(self, payload: dict):
            try:
                rates = payload.get("rates", {}) or {}
                threshold = float(payload.get("threshold", THRESHOLD_USD_DEFAULT))
                rate_sheet = payload.get("rate_sheet", RATE_SHEET_NAME)
                source = payload.get("source", "exchange_vs_cny")
            except Exception:
                rates = {}
                threshold = float(THRESHOLD_USD_DEFAULT)
                rate_sheet = RATE_SHEET_NAME
                source = "exchange_vs_cny"

            source_text = "来源：EXCHANGE VS CNY 推导（不依赖 LIMIT 公式）" if source == "exchange_vs_cny" else "来源：LIMIT AMOUNT 推导"
            lines = [
                f"USD 阈值（自动识别）：{threshold:,.2f}",
                f"rate 表：{rate_sheet}",
                source_text,
                "汇率（外币/美元）：",
            ]
            for ccy, r in sorted(rates.items()):
                try:
                    lines.append(f"{ccy}: {float(r):.6f}")
                except Exception:
                    lines.append(f"{ccy}: {r}")
            self.rate_box.setPlainText("\n".join(lines))

        def _prompt_missing_rates(self, missing_list: list):
            missing = [str(x).strip().upper() for x in (missing_list or []) if str(x).strip()]
            dlg = MissingRatesDialog(missing, parent=self)
            if hasattr(dlg, "exec"):
                dlg.exec()
            else:
                dlg.exec_()
            self.missing_rates_result.emit({"rates": dlg.result_rates()})

        def _handle_finished(self, payload: dict):
            count = int(payload.get("count", 0))
            threshold = float(payload.get("threshold", THRESHOLD_USD_DEFAULT))
            out_sheet = payload.get("output_sheet", OUTPUT_SHEET_NAME)
            missing = payload.get("missing", []) or []

            msg = f"已完成导出：{out_sheet}\n阈值：{threshold:,.2f}\n命中记录：{count}"
            if missing:
                msg += "\n\n以下币种仍缺汇率（对应记录不会参与筛选）：\n" + ", ".join(missing)

            QtWidgets.QMessageBox.information(self, "完成", msg)
            self.status_label.setText("处理完成。")
            self._set_running(False)
            open_file_path(self.path_edit.text().strip())

        def _handle_error(self, message: str):
            QtWidgets.QMessageBox.critical(self, "错误", message)
            self.status_label.setText("处理失败。")
            self._set_running(False)

        def _cleanup_worker(self):
            if self._worker is not None:
                self._worker.deleteLater()
                self._worker = None
            if self._worker_thread is not None:
                self._worker_thread.deleteLater()
                self._worker_thread = None

    app = QtWidgets.QApplication(sys.argv)
    try:
        app.setStyle("Fusion")
    except Exception:
        pass

    window = MainWindow()
    window.show()

    if hasattr(app, "exec"):
        app.exec()
    else:
        app.exec_()
    return True


def main():
    if not launch_ui():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
