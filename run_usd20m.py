import os
import subprocess
import sys
from typing import Optional

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter

# =========================
# 默认参数
# =========================
THRESHOLD_USD = 20_000_000
OUTPUT_SHEET_NAME = "USD_over_20M"
RATE_SHEET_NAME = "rate"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ICON_PATH = os.path.join(BASE_DIR, "app.ico")
LOGO_SRC_PATH = os.path.join(BASE_DIR, "ing-logo.png")


# =========================
# 工具函数
# =========================
def parse_amount_input(text: str) -> Optional[float]:
    if text is None:
        return None
    raw = str(text).strip()
    if not raw:
        return None

    s = raw.upper().replace(",", "").replace(" ", "")
    multiplier = 1.0
    if s.endswith("B"):
        multiplier = 1_000_000_000.0
        s = s[:-1]
    elif s.endswith("M"):
        multiplier = 1_000_000.0
        s = s[:-1]
    elif s.endswith("K"):
        multiplier = 1_000.0
        s = s[:-1]

    try:
        value = float(s) * multiplier
    except ValueError:
        return None

    if value <= 0:
        return None
    return value


def parse_manual_rates(text: str) -> dict:
    if text is None:
        return {}
    lines = [line.strip() for line in str(text).splitlines()]
    rates = {}
    for line in lines:
        if not line:
            continue
        if "#" in line:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue

        parts = None
        for sep in ("=", ":"):
            if sep in line:
                parts = [p.strip() for p in line.split(sep, 1)]
                break
        if parts is None:
            tokens = line.split()
            if len(tokens) >= 2:
                parts = [tokens[0], tokens[1]]

        if not parts or len(parts) < 2:
            continue

        ccy = parts[0].strip().upper()
        if len(ccy) != 3:
            continue

        val = parts[1].strip().replace(",", "").replace(" ", "")
        try:
            rate_val = float(val)
        except ValueError:
            continue

        if rate_val > 0:
            rates[ccy] = rate_val

    return rates


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
    """
    用于计算的金额解析：去逗号/空格，转数值；不改原 df 的 Transfer Amount（只返回计算用序列）
    """
    s = series.astype(str).str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def compute_usd_from_abs(abs_amount: pd.Series, ccy: pd.Series, fx_rate_local_per_usd: dict) -> pd.Series:
    """USD = abs(外币金额) / (外币/美元)"""
    ccy_upper = ccy.astype(str).str.upper().str.strip()
    rate = ccy_upper.map(fx_rate_local_per_usd)  # 外币/美元
    return abs_amount / rate


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

    # 保留原顺序
    df["__row_order"] = range(len(df))

    mask = df["Product Type"].astype(str).str.strip().isin(["Cash", "StructuredFlows"])
    df_need = df[mask].copy()
    df_keep = df[~mask].copy()

    if df_need.empty:
        return df_keep.drop(columns=["__row_order"], errors="ignore")

    df_need["Trade Id"] = df_need["Trade Id"].astype(str)

    kept_parts = []
    for trade_id, g in df_need.groupby("Trade Id", dropna=False, sort=False):
        g_sorted = g.sort_values("__row_order")

        g_none = g_sorted[g_sorted["Transfer Type"].astype(str).str.strip().str.upper() == "NONE"]
        if not g_none.empty:
            # NONE 有多少保留多少
            kept_parts.append(g_none)
        else:
            # 没有 NONE 就取第一条
            kept_parts.append(g_sorted.iloc[[0]])

    picked = pd.concat(kept_parts, ignore_index=False)

    out = pd.concat([df_keep, picked], ignore_index=True)
    out = out.sort_values("__row_order").drop(columns=["__row_order"], errors="ignore").reset_index(drop=True)
    return out


def autofit_column_width(ws, min_width=8, max_width=60, extra=2):
    """
    根据单元格内容长度自适应列宽
    - min_width: 最小列宽
    - max_width: 最大列宽（避免超宽）
    - extra: 额外留白
    """
    col_max = {}
    for row in ws.iter_rows(values_only=True):
        for j, val in enumerate(row, start=1):
            if val is None:
                length = 0
            else:
                length = len(str(val))
            col_max[j] = max(col_max.get(j, 0), length)

    for j, max_len in col_max.items():
        width = max(min_width, min(max_width, max_len + extra))
        ws.column_dimensions[get_column_letter(j)].width = width


def write_df_to_existing_workbook(file_path: str, sheet_name: str, df: pd.DataFrame):
    """写入到同一个 Excel 的最后一个sheet（若同名则覆盖），并自动调列宽"""
    wb = load_workbook(file_path)

    if sheet_name in wb.sheetnames:
        wb.remove(wb[sheet_name])

    ws = wb.create_sheet(title=sheet_name)

    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    ws.freeze_panes = "A2"
    autofit_column_width(ws)

    wb.save(file_path)


def load_fx_rates_from_rate_sheet(file_path: str, default_threshold_usd: float) -> tuple[dict, float, str]:
    """从 rate sheet 读取 LIMIT AMOUNT，推导 外币/美元 汇率，并返回 USD 阈值"""

    def norm_key(text: str) -> str:
        return str(text).strip().lower().replace(" ", "").replace("_", "")

    def extract_limit_amounts_from_raw(raw_df: pd.DataFrame, header_row_idx: Optional[int]) -> dict:
        if raw_df.empty:
            return {}

        ccy_idx = 0
        limit_idx = None
        if header_row_idx is not None:
            header_cells = raw_df.iloc[header_row_idx].tolist()
            for idx, cell in enumerate(header_cells):
                if str(cell).strip().upper() == "CCY":
                    ccy_idx = idx
                    break
            for idx, cell in enumerate(header_cells):
                if "LIMIT" in str(cell).strip().upper():
                    limit_idx = idx
                    break

        if limit_idx is None:
            limit_idx = 2 if raw_df.shape[1] > 2 else 1

        start_idx = header_row_idx + 1 if header_row_idx is not None else 0
        limits = {}
        for _, row in raw_df.iloc[start_idx:].iterrows():
            ccy = str(row.iloc[ccy_idx]).strip().upper()
            if not ccy or ccy == "CCY" or len(ccy) != 3:
                continue
            val = row.iloc[limit_idx] if limit_idx < len(row) else None
            val_num = pd.to_numeric(str(val).replace(",", "").replace(" ", ""), errors="coerce")
            if pd.notna(val_num) and val_num > 0:
                limits[ccy] = float(val_num)
        return limits

    xls = pd.ExcelFile(file_path, engine="openpyxl")
    rate_sheet = None
    for name in xls.sheet_names:
        if str(name).strip().lower() == RATE_SHEET_NAME:
            rate_sheet = name
            break

    if rate_sheet is None:
        for name in xls.sheet_names:
            try:
                raw_head = pd.read_excel(file_path, sheet_name=name, header=None, engine="openpyxl", nrows=20)
            except Exception:
                continue
            for _, row in raw_head.iterrows():
                if str(row.iloc[0]).strip().upper() == "CCY":
                    rate_sheet = name
                    break
            if rate_sheet:
                break

    if rate_sheet is None:
        raise ValueError("未找到 rate sheet，请确认文件包含 rate 表。")

    raw = pd.read_excel(file_path, sheet_name=rate_sheet, header=None, engine="openpyxl")

    threshold_usd = default_threshold_usd

    for _, row in raw.iterrows():
        if str(row.iloc[0]).strip().upper() != "USD":
            continue
        val2 = row.iloc[1] if len(row) > 1 else None
        val3 = row.iloc[2] if len(row) > 2 else None

        val2_num = pd.to_numeric(str(val2).replace(",", "").replace(" ", ""), errors="coerce")
        val3_num = pd.to_numeric(str(val3).replace(",", "").replace(" ", ""), errors="coerce")

        if pd.notna(val2_num) and (pd.isna(val3) or pd.isna(val3_num)) and val2_num > 1000:
            threshold_usd = float(val2_num)
            break

    if threshold_usd == default_threshold_usd:
        for _, row in raw.iterrows():
            if str(row.iloc[0]).strip().upper() != "USD":
                continue
            val2 = row.iloc[1] if len(row) > 1 else None
            val3 = row.iloc[2] if len(row) > 2 else None

            val2_num = pd.to_numeric(str(val2).replace(",", "").replace(" ", ""), errors="coerce")
            val3_num = pd.to_numeric(str(val3).replace(",", "").replace(" ", ""), errors="coerce")

            if pd.notna(val3_num) and val3_num > 0 and (pd.isna(val2_num) or val2_num <= 2):
                threshold_usd = float(val3_num)
                break

    header_row = None
    for idx, row in raw.iterrows():
        first = str(row.iloc[0]).strip().upper()
        if first == "CCY":
            header_row = idx
            break

    if header_row is None:
        header_row = 0

    rates_limit_map = {}
    try:
        df = pd.read_excel(file_path, sheet_name=rate_sheet, header=header_row, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        col_norm = {norm_key(c): c for c in df.columns}

        ccy_col = None
        for key in ["ccy", "currency", "curr"]:
            if key in col_norm:
                ccy_col = col_norm[key]
                break
        if not ccy_col:
            for key, orig in col_norm.items():
                if "ccy" in key:
                    ccy_col = orig
                    break

        limit_col = None
        for key in ["limitamount", "limitamt", "limit"]:
            if key in col_norm:
                limit_col = col_norm[key]
                break
        if not limit_col:
            for key, orig in col_norm.items():
                if "limit" in key and "amount" in key:
                    limit_col = orig
                    break

        if ccy_col and limit_col:
            df = df[[ccy_col, limit_col]].copy()
            df[ccy_col] = df[ccy_col].astype(str).str.strip().str.upper()
            df[limit_col] = (
                df[limit_col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
            )
            df[limit_col] = pd.to_numeric(df[limit_col], errors="coerce")
            df = df.dropna(subset=[ccy_col, limit_col])
            df = df[df[ccy_col] != "CCY"]
            rates_limit_map = {row[ccy_col]: float(row[limit_col]) for _, row in df.iterrows()}
    except Exception:
        rates_limit_map = {}

    if not rates_limit_map:
        rates_limit_map = extract_limit_amounts_from_raw(raw, header_row)

    if not rates_limit_map:
        raise ValueError("rate sheet 未读取到有效汇率数据。")

    if not threshold_usd or threshold_usd <= 0:
        threshold_usd = default_threshold_usd

    rates = {ccy: amt / threshold_usd for ccy, amt in rates_limit_map.items()}
    rates.setdefault("USD", 1.0)

    return rates, threshold_usd, rate_sheet


# =========================
# 主处理逻辑
# =========================
def process_workbook(
    file_path: str,
    progress_cb=None,
    status_cb=None,
    rates_cb=None,
    missing_cb=None,
    manual_threshold: Optional[str] = None,
    manual_rates_text: Optional[str] = None,
) -> int:
    def report_status(msg: str):
        if status_cb:
            status_cb(msg)

    def report_progress(step: int, total: int):
        if progress_cb:
            progress_cb(step, total)

    report_status("读取汇率...")
    manual_threshold_value = parse_amount_input(manual_threshold or "")
    manual_rates = parse_manual_rates(manual_rates_text or "")

    try:
        fx_rate_local_per_usd, threshold_usd, rate_sheet = load_fx_rates_from_rate_sheet(
            file_path, THRESHOLD_USD
        )
        sheet_source = "sheet"
    except ValueError as exc:
        if manual_rates:
            fx_rate_local_per_usd = {}
            threshold_usd = manual_threshold_value or THRESHOLD_USD
            rate_sheet = "手动输入"
            sheet_source = "manual"
        else:
            raise ValueError("未读取到 rate 表，请在手动汇率中输入后重试。") from exc

    if manual_threshold_value:
        threshold_usd = manual_threshold_value

    if manual_rates:
        fx_rate_local_per_usd.update(manual_rates)
        if sheet_source == "sheet":
            sheet_source = "sheet+manual"

    fx_rate_local_per_usd.setdefault("USD", 1.0)

    if rates_cb:
        rates_cb(
            {
                "rates": fx_rate_local_per_usd,
                "threshold_usd": threshold_usd,
                "sheet_name": rate_sheet,
                "source": sheet_source,
            }
        )

    xls = pd.ExcelFile(file_path, engine="openpyxl")
    data_sheets = [s for s in xls.sheet_names if s not in (rate_sheet, OUTPUT_SHEET_NAME)]
    sheet_names = data_sheets[:3]

    if not sheet_names:
        write_df_to_existing_workbook(file_path, OUTPUT_SHEET_NAME, pd.DataFrame())
        report_status("未找到可处理的表，已生成空结果。")
        report_progress(1, 1)
        return 0

    total_steps = len(sheet_names) + 1
    report_progress(0, total_steps)

    results = []

    missing_ccy_set = set()

    for idx, sh in enumerate(sheet_names, start=1):
        report_status(f"处理中：{sh}")
        df = pd.read_excel(file_path, sheet_name=sh, engine="openpyxl")
        df = normalize_columns(df)

        must_cols = ["Transfer Amount", "SettleCurrency", "Product Type", "Trade Id", "Transfer Type"]
        missing = [c for c in must_cols if c not in df.columns]
        if missing:
            print(f"[WARN] Sheet '{sh}' 缺少列 {missing}，已跳过。")
            report_progress(idx, total_steps)
            continue

        amt_num = parse_amount_to_float(df["Transfer Amount"])
        abs_amt = amt_num.abs()

        ccy_upper = df["SettleCurrency"].astype(str).str.upper().str.strip()
        rate = ccy_upper.map(fx_rate_local_per_usd)
        missing_ccy = sorted(set(ccy_upper[rate.isna()].tolist()))
        missing_ccy = [c for c in missing_ccy if c and c != "NAN"]
        if missing_ccy:
            missing_ccy_set.update(missing_ccy)
            print(f"[WARN] 未找到汇率：{missing_ccy}")

        usd_amt = abs_amt / rate

        df_big = df[usd_amt > threshold_usd].copy()
        if df_big.empty:
            report_progress(idx, total_steps)
            continue

        df_big = apply_tradeid_rule_keep_all_none(df_big)
        df_big.loc[:, "Transfer Amount"] = abs_amt.loc[df_big.index]

        results.append(df_big)
        report_progress(idx, total_steps)

    report_status("写入结果...")

    if missing_ccy_set and missing_cb:
        missing_cb(sorted(missing_ccy_set))

    if results:
        out_df = pd.concat(results, ignore_index=True)
        write_df_to_existing_workbook(file_path, OUTPUT_SHEET_NAME, out_df)
        report_progress(total_steps, total_steps)
        return len(out_df)

    out_df = pd.DataFrame()
    write_df_to_existing_workbook(file_path, OUTPUT_SHEET_NAME, out_df)
    report_progress(total_steps, total_steps)
    return 0


def open_file_path(file_path: str) -> None:
    if not file_path:
        return
    if not os.path.exists(file_path):
        return
    try:
        if sys.platform.startswith("win"):
            os.startfile(file_path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.run(["open", file_path], check=False)
        else:
            subprocess.run(["xdg-open", file_path], check=False)
    except Exception as exc:
        print(f"[WARN] 自动打开失败: {exc}")


# =========================
# UI 相关
# =========================
def load_qt_modules():
    try:
        from PySide6 import QtCore, QtGui, QtWidgets
        return "PySide6", QtCore, QtGui, QtWidgets, QtCore.Signal
    except Exception:
        pass

    try:
        from PyQt6 import QtCore, QtGui, QtWidgets
        return "PyQt6", QtCore, QtGui, QtWidgets, QtCore.pyqtSignal
    except Exception:
        pass

    try:
        from PyQt5 import QtCore, QtGui, QtWidgets
        return "PyQt5", QtCore, QtGui, QtWidgets, QtCore.pyqtSignal
    except Exception:
        return None


def launch_ui() -> bool:
    qt_modules = load_qt_modules()
    if not qt_modules:
        print("UI 依赖 Qt (PySide6 / PyQt6 / PyQt5)，当前环境未安装。")
        print("请先执行: python -m pip install PySide6")
        return False

    _qt_name, QtCore, QtGui, QtWidgets, Signal = qt_modules
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

    class Worker(QtCore.QObject):
        progress = Signal(int)
        status = Signal(str)
        rates = Signal(object)
        missing = Signal(object)
        finished = Signal(int)
        error = Signal(str)

        def __init__(self, file_path: str, manual_threshold: str, manual_rates_text: str):
            super().__init__()
            self.file_path = file_path
            self.manual_threshold = manual_threshold
            self.manual_rates_text = manual_rates_text

        def run(self):
            def progress_cb(step: int, total: int):
                percent = int(step / max(total, 1) * 100)
                self.progress.emit(percent)

            def status_cb(msg: str):
                self.status.emit(msg)

            def rates_cb(rates: dict):
                self.rates.emit(rates)

            def missing_cb(missing_list: list):
                self.missing.emit(missing_list)

            try:
                count = process_workbook(
                    self.file_path,
                    progress_cb=progress_cb,
                    status_cb=status_cb,
                    rates_cb=rates_cb,
                    missing_cb=missing_cb,
                    manual_threshold=self.manual_threshold,
                    manual_rates_text=self.manual_rates_text,
                )
            except Exception as exc:
                self.error.emit(str(exc))
                return

            self.finished.emit(count)

    class MainWindow(QtWidgets.QWidget):
        def __init__(self):
            super().__init__()
            self._worker_thread = None
            self._worker = None
            self.threshold_usd = THRESHOLD_USD
            self._build_ui()

        def _build_ui(self):
            self.setWindowTitle("USD 大额筛选")
            self.setMinimumSize(760, 520)
            self.resize(880, 580)

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
            panel.setMaximumWidth(760)

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
            subtitle_label = QtWidgets.QLabel("从 rate 表读取汇率，按 USD 阈值筛选记录")
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

            hint_label = QtWidgets.QLabel(
                f"输出 Sheet：{OUTPUT_SHEET_NAME}（Transfer Amount 将输出为绝对值）"
            )
            hint_label.setObjectName("hint")
            hint_label.setFont(small_font)
            panel_layout.addWidget(hint_label)

            threshold_label = QtWidgets.QLabel("USD 阈值（可选）")
            threshold_label.setFont(label_font)
            panel_layout.addWidget(threshold_label)

            self.threshold_edit = QtWidgets.QLineEdit()
            self.threshold_edit.setPlaceholderText("自动读取（如 30M / 30000000）")
            panel_layout.addWidget(self.threshold_edit)

            manual_rate_label = QtWidgets.QLabel("手动汇率（可选，格式：JPY=156.90）")
            manual_rate_label.setFont(label_font)
            panel_layout.addWidget(manual_rate_label)

            self.manual_rate_box = QtWidgets.QPlainTextEdit()
            self.manual_rate_box.setObjectName("manualRateBox")
            self.manual_rate_box.setMinimumHeight(70)
            self.manual_rate_box.setMaximumHeight(110)
            self.manual_rate_box.setFont(small_font)
            panel_layout.addWidget(self.manual_rate_box)

            rate_label = QtWidgets.QLabel("读取汇率（外币/美元）")
            rate_label.setFont(label_font)
            panel_layout.addWidget(rate_label)

            self.rate_box = QtWidgets.QPlainTextEdit()
            self.rate_box.setObjectName("rateBox")
            self.rate_box.setReadOnly(True)
            self.rate_box.setMinimumHeight(90)
            self.rate_box.setMaximumHeight(120)
            self.rate_box.setFont(small_font)
            self.rate_box.setPlainText("尚未读取。")
            panel_layout.addWidget(self.rate_box)

            action_row = QtWidgets.QHBoxLayout()
            self.run_btn = QtWidgets.QPushButton("开始筛选")
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
                QPlainTextEdit#manualRateBox {
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
                QPushButton#primaryButton:disabled {
                    background-color: #2B6E63;
                    color: #0A1B22;
                }
                QPushButton#secondaryButton {
                    background-color: #132833;
                    color: #E6F1F5;
                    border: 1px solid #23414D;
                    border-radius: 10px;
                    padding: 8px 16px;
                }
                QPushButton#secondaryButton:hover { border-color: #44E6C2; }
                QPushButton#secondaryButton:disabled {
                    color: #6C828B;
                    border-color: #1C2F38;
                }
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
                self,
                "选择 Excel 文件",
                BASE_DIR,
                "Excel 文件 (*.xlsx *.xlsm *.xls)",
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

            manual_threshold_text = self.threshold_edit.text().strip()
            if manual_threshold_text and parse_amount_input(manual_threshold_text) is None:
                QtWidgets.QMessageBox.warning(self, "提示", "USD 阈值格式不正确，请重新输入。")
                return

            manual_rates_text = self.manual_rate_box.toPlainText().strip()
            if manual_rates_text:
                parsed_manual = parse_manual_rates(manual_rates_text)
                if not parsed_manual:
                    QtWidgets.QMessageBox.warning(
                        self,
                        "提示",
                        "手动汇率格式不正确，请按示例填写：JPY=156.90",
                    )
                    return

            self.progress_bar.setValue(0)
            self.status_label.setText("准备处理…")
            self.rate_box.setPlainText("读取汇率中…")
            self._set_running(True)

            self._worker_thread = QtCore.QThread()
            self._worker = Worker(file_path, manual_threshold_text, manual_rates_text)
            self._worker.moveToThread(self._worker_thread)

            self._worker_thread.started.connect(self._worker.run)
            self._worker.progress.connect(self.progress_bar.setValue)
            self._worker.status.connect(self.status_label.setText)
            self._worker.rates.connect(self._handle_rates)
            self._worker.missing.connect(self._handle_missing)
            self._worker.finished.connect(self._handle_finished)
            self._worker.error.connect(self._handle_error)
            self._worker.finished.connect(self._worker_thread.quit)
            self._worker.error.connect(self._worker_thread.quit)
            self._worker_thread.finished.connect(self._cleanup_worker)

            self._worker_thread.start()

        def _handle_finished(self, count: int):
            if count:
                msg = f"完成：已写入 {count} 条记录到 {OUTPUT_SHEET_NAME}"
            else:
                msg = f"完成：未找到超过 {self.threshold_usd:,.0f} USD 的记录"
            QtWidgets.QMessageBox.information(self, "完成", msg)
            self.status_label.setText("处理完成。")
            self._set_running(False)
            open_file_path(self.path_edit.text().strip())

        def _handle_error(self, message: str):
            QtWidgets.QMessageBox.critical(self, "错误", message)
            self.status_label.setText("处理失败。")
            self._set_running(False)

        def _handle_missing(self, missing_list: list):
            if not missing_list:
                return
            msg = "未找到以下币种汇率：\n" + ", ".join(missing_list)
            QtWidgets.QMessageBox.warning(self, "提示", msg)
            self.status_label.setText("部分币种未匹配到汇率。")

        def _handle_rates(self, rates: dict):
            if not rates or not isinstance(rates, dict):
                self.rate_box.setPlainText("未读取到汇率。")
                return
            rate_map = rates.get("rates") or {}
            threshold_usd = rates.get("threshold_usd", THRESHOLD_USD)
            sheet_name = rates.get("sheet_name", RATE_SHEET_NAME)
            source = rates.get("source", "sheet")
            self.threshold_usd = threshold_usd
            if source == "manual":
                source_text = "来源: 手动输入"
            elif source == "sheet+manual":
                source_text = "来源: rate 表 + 手动输入"
            else:
                source_text = "来源: rate 表"
            lines = [
                f"USD 阈值: {threshold_usd:,.0f}",
                f"rate 表: {sheet_name}",
                source_text,
                "汇率（外币/美元）:",
            ]
            for ccy, rate in sorted(rate_map.items()):
                try:
                    lines.append(f"{ccy}: {float(rate):.6f}")
                except Exception:
                    lines.append(f"{ccy}: {rate}")
            self.rate_box.setPlainText("\n".join(lines))

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


def run_cli(file_path: Optional[str] = None) -> int:
    if not file_path:
        try:
            file_path = input("请输入 Excel 文件路径: ").strip()
        except EOFError:
            file_path = ""

    if not file_path:
        print("未提供文件路径，退出。")
        return 1

    if not os.path.exists(file_path):
        print("文件不存在，请检查路径。")
        return 1

    last_percent = {"value": -1}

    def progress_cb(step: int, total: int):
        percent = int(step / max(total, 1) * 100)
        if percent != last_percent["value"]:
            print(f"进度: {percent}%")
            last_percent["value"] = percent

    def status_cb(msg: str):
        print(msg)

    threshold_holder = {"value": THRESHOLD_USD}

    def rates_cb(rates: dict):
        if not rates or not isinstance(rates, dict):
            print("未读取到汇率。")
            return
        rate_map = rates.get("rates") or {}
        threshold_holder["value"] = rates.get("threshold_usd", THRESHOLD_USD)
        sheet_name = rates.get("sheet_name", RATE_SHEET_NAME)
        source = rates.get("source", "sheet")
        print(f"USD 阈值: {threshold_holder['value']:,.0f}")
        print(f"rate 表: {sheet_name}")
        if source == "manual":
            print("来源: 手动输入")
        elif source == "sheet+manual":
            print("来源: rate 表 + 手动输入")
        else:
            print("来源: rate 表")
        print("汇率（外币/美元）：")
        for ccy, rate in sorted(rate_map.items()):
            try:
                print(f"  {ccy}: {float(rate):.6f}")
            except Exception:
                print(f"  {ccy}: {rate}")

    def missing_cb(missing_list: list):
        if not missing_list:
            return
        print("未找到以下币种汇率：", ", ".join(missing_list))

    try:
        count = process_workbook(
            file_path,
            progress_cb=progress_cb,
            status_cb=status_cb,
            rates_cb=rates_cb,
            missing_cb=missing_cb,
        )
    except Exception as exc:
        print(f"处理失败: {exc}")
        return 1

    if count:
        print(f"完成：已写入 {count} 条记录到 {OUTPUT_SHEET_NAME}")
    else:
        print(f"完成：未找到超过 {threshold_holder['value']:,.0f} USD 的记录")
    open_file_path(file_path)
    return 0


def main():
    args = sys.argv[1:]
    file_path = None
    force_cli = False

    for arg in args:
        if arg in ("--cli", "-c"):
            force_cli = True
        elif not arg.startswith("-") and file_path is None:
            file_path = arg

    if force_cli:
        raise SystemExit(run_cli(file_path))

    if not launch_ui():
        print("已切换到命令行模式。可用参数：python run_usd20m.py --cli <excel_path>")
        raise SystemExit(run_cli(file_path))


if __name__ == "__main__":
    main()
