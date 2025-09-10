#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import time
import warnings
from typing import List, Dict, Tuple, Optional

import pandas as pd
import requests
import streamlit as st
from datetime import datetime
from urllib.parse import quote
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# ===== tijdzone helpers (robust, ook als host in UTC draait) =====
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # fallback voor oudere omgevingen

def now_naive_in_tz(tz_str: str) -> pd.Timestamp:
    """Echte kloktijd in deze tz, zonder tz-info (naive Pandas Timestamp)."""
    return pd.Timestamp(datetime.now(ZoneInfo(tz_str))).tz_localize(None)

def now_aware_in_tz(tz_str: str) -> pd.Timestamp:
    """Huidige tijd als tz-aware pandas Timestamp in gewenste tz."""
    return pd.Timestamp(datetime.now(ZoneInfo(tz_str)))

# ===== Streamlit helpers (reset na download) =====
def _mark_downloaded():
    st.session_state["_downloaded"] = True

def _safe_rerun():
    # Streamlit 1.30+: st.rerun; oudere versies: st.experimental_rerun
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()

# Onderdruk macOS LibreSSL waarschuwing
warnings.filterwarnings("ignore", message=r"urllib3 v2 only supports OpenSSL.*", category=Warning, module=r"urllib3\.__init__")

# Rich text detectie (voor rode annotaties bovenaan)
try:
    from openpyxl.cell.rich_text import CellRichText, TextBlock
    from openpyxl.cell.text import InlineFont
    _HAS_RICHTEXT = True
except Exception:
    _HAS_RICHTEXT = False

# === VASTE INSTELLINGEN ===
DEFAULT_CLIENT_ID = "K662D1WXrt"
TZ = "Europe/Amsterdam"
DAYS_AHEAD = 60
WEEK_OFFSET = -1  # ophaal: vorige week + 60 dagen
FIELDS = "naam,datumvanaf,datumtot,tijdvanaf,tijdtot,lokatie,heledag"

BAR_CODES = ["445", "701", "741"]
CK_CODES  = ["442"]
WEEK_LABEL = "short"       # of "iso"
SAT_ONLY_CK = True         # CommissieKamer alleen zaterdag

DAYS_NL = ["Maandag","Dinsdag","Woensdag","Donderdag","Vrijdag","Zaterdag","Zondag"]
DAY_COLORS = {
    "Maandag":"FFDDEBF7","Dinsdag":"FFE2EFDA","Woensdag":"FFFFF2CC",
    "Donderdag":"FFFCE4D6","Vrijdag":"FFE7E6E6","Zaterdag":"FFE4DFEC","Zondag":"FFF8CBAD"
}
# Shifts per dag: (Tijd-van, Tijd-tot)
DEFAULT_SLOTS: Dict[str, List[Tuple[str, str]]] = {
    "Maandag":   [("18:00","19:00"), ("19:00","20:00")],
    "Dinsdag":   [("18:00","22:00")],
    "Woensdag":  [("18:00","19:00"), ("19:00","22:00")],
    "Donderdag": [("18:00","22:00")],
    "Vrijdag":   [("18:00","20:30"), ("20:30","23:00")],
    "Zaterdag":  [("07:30","10:00"), ("10:00","12:30"),
                  ("12:30","15:00"), ("15:00","17:30"),
                  ("17:30","20:00"), ("20:00","22:30")],
    "Zondag":    [("10:00","12:30"), ("12:30","15:00")],
}

# ---------- Helpers ----------
def month_short_nl(m:int) -> str:
    return ["jan","feb","mrt","apr","mei","jun","jul","aug","sept","okt","nov","dec"][m-1]

def build_urls(taskcodes: List[str],
               days: int,
               client_id: str,
               weekoffset: int = -1,
               fields: Optional[str] = FIELDS) -> List[str]:
    base = "https://data.sportlink.com/vrijwilligers"
    urls = []
    for code in taskcodes:
        url = (
            f"{base}?vrijwilligerstaakcode={code}"
            f"&aantaldagen={int(days)}"
            f"&client_id={client_id}"
            f"&weekoffset={int(weekoffset)}"
        )
        if fields:
            url += f"&fields={quote(fields)}"
        urls.append(url)
    return urls

def http_get_json(url: str, timeout: int = 30, max_retries: int = 3, backoff_factor: float = 0.8):
    headers = {"User-Agent": "CKC-Rooster/Streamlit", "Accept": "application/json"}
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout, headers=headers)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "items" in data:
                data = data["items"]
            if not isinstance(data, list):
                raise ValueError("Unexpected JSON: expected a list of records.")
            return data
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(backoff_factor * (2 ** (attempt - 1)))
            else:
                raise
    raise last_err

def _pick(colnames, candidates):
    for c in candidates:
        if c in colnames: return c
    lower = {c.lower(): c for c in colnames}
    for c in candidates:
        if c.lower() in lower: return lower[c.lower()]
    return None

def normalize_dataframe(data, tz_str: str):
    """df: Naam, Datum (tz-naive), Dag, Tijd vanaf, Tijd tot, Week, ISO_Year."""
    df_raw = pd.DataFrame(data)
    cols = df_raw.columns.tolist()

    c_naam = _pick(cols, ["Naam","naam","Vrijwilliger","vrijwilliger","vrijwilligerNaam","displayName"])
    c_dv   = _pick(cols, ["Datum vanaf","datumvanaf","start","Start","DatumVanaf","startDatumTijd","startDateTime"])
    c_dt   = _pick(cols, ["Datum tot","datumtot","eind","Eind","DatumTot","eindDatumTijd","endDateTime"])
    c_tv   = _pick(cols, ["Tijd vanaf","tijdvanaf","startTijd","starttijd","StartTijd"])
    c_tt   = _pick(cols, ["Tijd tot","tijdtot","eindtijd","EindTijd","endTijd","TijdTot"])

    df = df_raw.copy()
    if c_tv is None and c_dv is not None:
        tmp = pd.to_datetime(df[c_dv], errors="coerce", utc=True)
        df["Tijd vanaf"] = tmp.dt.tz_convert(tz_str).dt.strftime("%H:%M"); c_tv = "Tijd vanaf"
    if c_tt is None and c_dt is not None:
        tmp2 = pd.to_datetime(df[c_dt], errors="coerce", utc=True)
        df["Tijd tot"] = tmp2.dt.tz_convert(tz_str).dt.strftime("%H:%M"); c_tt = "Tijd tot"

    out = pd.DataFrame({
        "Naam": df[c_naam] if c_naam else "",
        "Datum vanaf": df[c_dv] if c_dv else "",
        "Datum tot": df[c_dt] if c_dt else "",
        "Tijd vanaf": df[c_tv] if c_tv else "",
        "Tijd tot": df[c_tt] if c_tt else "",
    })

    dat = pd.to_datetime(out["Datum vanaf"], errors="coerce", utc=True)
    dat = dat.dt.tz_convert(tz_str).dt.tz_localize(None)  # tz-naive
    out["Datum"] = dat
    out = out.dropna(subset=["Datum"])

    out["Week"] = out["Datum"].dt.isocalendar().week.astype(int)
    out["ISO_Year"] = out["Datum"].dt.isocalendar().year.astype(int)
    out["Weekdag_num"] = out["Datum"].dt.weekday
    out["Dag"] = out["Weekdag_num"].map(lambda i: DAYS_NL[i] if pd.notna(i) else None)

    t_from = pd.to_datetime(out["Tijd vanaf"], format="%H:%M", errors="coerce")
    out["Tijd vanaf"] = t_from.dt.strftime("%H:%M").fillna("")
    t_to = pd.to_datetime(out["Tijd tot"], format="%H:%M", errors="coerce")
    out["Tijd tot"] = t_to.dt.strftime("%H:%M").fillna("")
    return out

def filter_from_current_week(df: pd.DataFrame, tz_str: str) -> pd.DataFrame:
    """Houd alleen rijen met Datum-dag >= maandag (00:00) van de huidige week (tz-naive)."""
    now_naive = now_naive_in_tz(tz_str)
    monday_naive = (now_naive - pd.Timedelta(days=int(now_naive.weekday()))).normalize()
    return df[df["Datum"].dt.normalize() >= monday_naive].copy()

def monday_of_week(d: pd.Timestamp) -> pd.Timestamp:
    return d - pd.Timedelta(days=int(d.weekday()))

def derive_weeks(df: pd.DataFrame, tz_str: str, horizon_weeks_if_empty=4):
    if not df.empty:
        weeks_pairs = sorted({(int(y), int(w)) for y, w in zip(df["ISO_Year"], df["Week"])})
        week_mondays = {}
        for (y, w) in weeks_pairs:
            d0 = df.loc[(df["ISO_Year"] == y) & (df["Week"] == w), "Datum"].iloc[0]
            week_mondays[(y, w)] = monday_of_week(d0)
        return weeks_pairs, week_mondays

    now = now_naive_in_tz(tz_str)
    mon0 = now - pd.Timedelta(days=int(now.weekday()))
    weeks_pairs = []; week_mondays = {}
    for i in range(horizon_weeks_if_empty):
        mon = (mon0 + pd.Timedelta(days=7*i)).normalize()
        iso = mon.isocalendar(); pair = (int(iso.year), int(iso.week))
        weeks_pairs.append(pair); week_mondays[pair] = mon
    return weeks_pairs, week_mondays

def build_matrix(df: pd.DataFrame,
                 slots: Dict[str, List[Tuple[str,str]]],
                 tz_str: str,
                 days_subset=None,
                 horizon_weeks_if_empty=4,
                 week_label_style: str="short",
                 extra_weeks_pairs: Optional[List[Tuple[int,int]]]=None) -> pd.DataFrame:

    if days_subset is not None:
        df = df[df["Dag"].isin(days_subset)].copy()

    weeks_pairs, week_mondays = derive_weeks(df, tz_str, horizon_weeks_if_empty)

    # Huidige week altijd aanwezig (tz-naive)
    now_naive = now_naive_in_tz(tz_str)
    iso_now = now_naive.isocalendar()
    current_pair = (int(iso_now.year), int(iso_now.week))
    if current_pair not in weeks_pairs:
        weeks_pairs.append(current_pair)
        mon_cur = (now_naive - pd.Timedelta(days=int(now_naive.weekday()))).normalize()
        week_mondays[current_pair] = mon_cur
        weeks_pairs = sorted(weeks_pairs)

    if extra_weeks_pairs:
        for p in extra_weeks_pairs:
            if p not in weeks_pairs:
                weeks_pairs.append(p)
                y, w = p
                mon = pd.Timestamp.fromisocalendar(y, w, 1)
                week_mondays[p] = mon
        weeks_pairs = sorted(weeks_pairs)

    def week_label(pair: Tuple[int,int]) -> str:
        y, w = pair
        return f"{y}-W{w:02d}" if week_label_style == "iso" else f"Week {w}"

    days_to_use = DAYS_NL if days_subset is None else [d for d in DAYS_NL if d in days_subset]
    rows = []
    for d in days_to_use:
        rows.append((d, "", ""))  # header
        for (van, tot) in slots.get(d, []):
            rows.append((d, van, tot))

    matrix = pd.DataFrame(
        "",
        index=pd.MultiIndex.from_tuples(rows, names=["Dag","Tijd-van","Tijd-tot"]),
        columns=[week_label(p) for p in weeks_pairs]
    )

    # MultiIndex lexsorten in jouw dagvolgorde
    mi = matrix.index
    dag = pd.CategoricalIndex([tpl[0] for tpl in mi], categories=DAYS_NL, ordered=True, name="Dag")
    tvan = [tpl[1] for tpl in mi]
    ttot = [tpl[2] for tpl in mi]
    matrix.index = pd.MultiIndex.from_arrays([dag, tvan, ttot], names=["Dag","Tijd-van","Tijd-tot"])
    matrix.sort_index(level=[0,1,2], inplace=True)

    for p in weeks_pairs:
        mon = week_mondays[p]; col = week_label(p)
        for d in days_to_use:
            day_date = (mon + pd.Timedelta(days=DAYS_NL.index(d))).strftime("%d-%b")
            matrix.loc[(d,"",""), col] = f"{d} ({day_date})"

    for _, r in df.iterrows():
        d = r["Dag"]; t_from = r["Tijd vanaf"]; t_to = r["Tijd tot"]
        w = int(r["Week"]); y = int(r["ISO_Year"])
        if pd.isna(d) or pd.isna(w) or not t_from: continue
        if not t_to:
            for (van, tot) in slots.get(d, []):
                if van == t_from:
                    t_to = tot; break
        col = week_label((y, w))
        if (d in slots) and ((t_from, t_to) in slots.get(d, [])) and (col in matrix.columns):
            cur = matrix.loc[(d, t_from, t_to), col]
            name = str(r["Naam"]) if pd.notna(r["Naam"]) else ""
            matrix.loc[(d, t_from, t_to), col] = (cur + "\n" + name) if cur else name

    return matrix

def format_sheet(ws, matrix, slots: Dict[str, List[Tuple[str,str]]], tz_str: str):
    thin = Side(style="thin", color="FFAAAAAA")
    thick = Side(style="thick", color="FF000000")
    bold = Font(bold=True)
    wrap = Alignment(wrap_text=True, vertical="top")
    center = Alignment(horizontal="center", vertical="center")

    first_week_col_idx = 4  # D
    last_col_idx = first_week_col_idx + len(matrix.columns) - 1
    first_data_row = 2

    day_last_row = {}
    last_slot_for_day = {d: slots[d][-1] if slots.get(d) else ("","") for d in set([ix[0] for ix in matrix.index])}
    for r_idx, (d, van, tot) in enumerate(matrix.index, start=first_data_row):
        if van != "" and (van, tot) == last_slot_for_day.get(d, ("","")):
            day_last_row[d] = r_idx

    for r_idx, (d, van, tot) in enumerate(matrix.index, start=first_data_row):
        is_header = (van == "" and tot == "")
        for c_idx in range(1, last_col_idx+1):
            cell = ws.cell(row=r_idx, column=c_idx)
            cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)
            if is_header:
                fill = PatternFill(start_color=DAY_COLORS.get(d, "FFFFFFFF"),
                                   end_color=DAY_COLORS.get(d, "FFFFFFFF"),
                                   fill_type="solid")
                cell.fill = fill; cell.font = bold; cell.alignment = center
            else:
                cell.alignment = wrap
        if r_idx in day_last_row.values():
            for c_idx in range(1, last_col_idx+1):
                cell = ws.cell(row=r_idx, column=c_idx)
                cell.border = Border(left=cell.border.left, right=cell.border.right,
                                     top=cell.border.top, bottom=thick)

    for j, _ in enumerate(matrix.columns, start=first_week_col_idx):
        for r in range(1, ws.max_row+1):
            cell = ws.cell(row=r, column=j)
            cell.border = Border(left=thick, right=cell.border.right,
                                 top=cell.border.top, bottom=cell.border.bottom)

    # Kolombreedtes
    ws.column_dimensions['A'].width = 12  # Dag
    ws.column_dimensions['B'].width = 9   # Tijd-van
    ws.column_dimensions['C'].width = 9   # Tijd-tot
    for col_cells in ws.iter_cols(min_col=4, max_col=ws.max_column):
        max_len = 14
        for cell in col_cells:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_cells[0].column_letter].width = min(max(int(max_len*0.75)+2, 10), 24)

    # Timestamp in A1 — echte Amsterdamse kloktijd
    now = now_naive_in_tz(tz_str)
    stamp = f"{now.day} {month_short_nl(now.month)} {now.strftime('%H:%M')}"
    a1 = ws.cell(row=1, column=1); a1.value = stamp
    try: a1.font = Font(italic=True, color="FF666666")
    except Exception: pass

    ws.freeze_panes = "D2"

def apply_manual_annotations(ws, matrix, annotations, week_label_style: str, slots: Dict[str, List[Tuple[str,str]]]):
    cols = list(matrix.columns)
    col_index_by_label = {label: idx for idx, label in enumerate(cols, start=4)}  # D = eerste weekkolom

    def week_label(y, w): return f"{y}-W{w:02d}" if week_label_style == "iso" else f"Week {w}"

    row_index_by_key = {}; row_idx = 2
    for (d, van, tot) in matrix.index:
        row_index_by_key[(d, van, tot)] = row_idx; row_idx += 1

    for a in annotations:
        label = week_label(a["iso_year"], a["iso_week"])
        col_idx = col_index_by_label.get(label)
        # 'tot' afleiden uit slots
        tot = None
        for (v, t) in slots.get(a["day"], []):
            if v == a["time_from"]:
                tot = t; break
        if col_idx is None or tot is None:
            continue
        row_idx = row_index_by_key.get((a["day"], a["time_from"], tot))
        if row_idx is None:
            continue

        cell = ws.cell(row=row_idx, column=col_idx)
        cur = str(cell.value) if cell.value is not None else ""
        anno_text = a["text"].strip()

        used_richtext = False
        if _HAS_RICHTEXT:
            try:
                rt = CellRichText()
                rt.append(TextBlock(InlineFont(color="FF0000"), anno_text + "\n"))  # rood bovenaan
                if cur.strip():
                    rt.append("---\n")
                    rt.append(cur)
                cell.value = rt
                cell.alignment = Alignment(wrap_text=True, vertical="top")
                used_richtext = True
            except Exception:
                used_richtext = False

        if not used_richtext:
            if cur.strip():
                cell.value = f"{anno_text}\n---\n{cur}"
            else:
                cell.value = anno_text
            cell.font = Font(color="FF0000")
            cell.alignment = Alignment(wrap_text=True, vertical="top")

def parse_manual_text(text: str):
    entries = []
    if not text: return entries
    now_aware = now_aware_in_tz(TZ)
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"): continue
        parts = s.split()
        if len(parts) < 3: continue
        date_str, time_str = parts[0], parts[1]
        txt = " ".join(parts[2:]).strip()
        try:
            # maak een aware timestamp in Amsterdam
            dt_aware = pd.Timestamp(f"{date_str} {time_str}", tz=ZoneInfo(TZ))
        except Exception:
            continue
        if dt_aware < now_aware: continue
        day_name = DAYS_NL[int(dt_aware.weekday())]
        starts = [a for a,b in DEFAULT_SLOTS.get(day_name, [])]
        if time_str not in starts: continue
        iso = dt_aware.isocalendar()
        entries.append({
            "date": dt_aware.tz_convert(None),  # tz-naive voor Excel
            "time_from": time_str,
            "text": txt,
            "iso_year": int(iso.year),
            "iso_week": int(iso.week),
            "day": day_name,
        })
    return entries

def make_excel(df_bar, df_ck, annotations):
    extra_weeks_pairs = sorted({(a["iso_year"], a["iso_week"]) for a in annotations})
    matrix_bar = build_matrix(df_bar, slots=DEFAULT_SLOTS, tz_str=TZ,
                              days_subset=None, horizon_weeks_if_empty=4,
                              week_label_style=WEEK_LABEL, extra_weeks_pairs=extra_weeks_pairs)
    days_subset_ck = ["Zaterdag"] if SAT_ONLY_CK else None
    matrix_ck = build_matrix(df_ck, slots=DEFAULT_SLOTS, tz_str=TZ,
                             days_subset=days_subset_ck, horizon_weeks_if_empty=4,
                             week_label_style=WEEK_LABEL, extra_weeks_pairs=extra_weeks_pairs)

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        matrix_bar.to_excel(writer, sheet_name="BarRooster")
        ws_bar = writer.sheets["BarRooster"]
        format_sheet(ws_bar, matrix_bar, DEFAULT_SLOTS, TZ)
        apply_manual_annotations(ws_bar, matrix_bar, annotations, week_label_style=WEEK_LABEL, slots=DEFAULT_SLOTS)

        matrix_ck.to_excel(writer, sheet_name="CommissieKamer")
        ws_ck = writer.sheets["CommissieKamer"]
        format_sheet(ws_ck, matrix_ck, DEFAULT_SLOTS, TZ)
        apply_manual_annotations(ws_ck, matrix_ck, annotations, week_label_style=WEEK_LABEL, slots=DEFAULT_SLOTS)

    bio.seek(0)
    return bio

# =========================
# UI (simpel & mobiel)
# =========================
st.set_page_config(page_title="Rooster generator", page_icon="🗓️", layout="centered")
st.markdown("<h1 style='text-align:center;margin-bottom:0'>🗓️ CKC Rooster generator</h1>", unsafe_allow_html=True)
st.caption("Sportlink → Excel · vaste instellingen (Europe/Amsterdam, weekoffset=-1, gefilterd vanaf huidige week)")

manual_text = st.text_area(
    "Handmatige input (optioneel, één per regel)",
    placeholder="YYYY-MM-DD HH:MM tekst\n2025-09-12 20:30 klaverjassen",
    height=120
)

if st.button("Genereer rooster", use_container_width=True):
    try:
        with st.spinner("Ophalen en bouwen…"):
            urls_bar = build_urls(BAR_CODES, DAYS_AHEAD, DEFAULT_CLIENT_ID, weekoffset=WEEK_OFFSET, fields=FIELDS)
            urls_ck  = build_urls(CK_CODES,  DAYS_AHEAD, DEFAULT_CLIENT_ID, weekoffset=WEEK_OFFSET, fields=FIELDS)

            all_bar = sum([http_get_json(u) for u in urls_bar], [])
            all_ck  = sum([http_get_json(u) for u in urls_ck],  [])

            # Normalize + FILTER vanaf maandag van deze week (tz-naive)
            df_bar = filter_from_current_week(normalize_dataframe(all_bar, TZ), TZ)
            df_ck  = filter_from_current_week(normalize_dataframe(all_ck,  TZ), TZ)

            annotations = parse_manual_text(manual_text)
            xlsx = make_excel(df_bar, df_ck, annotations)

        st.success("Klaar! Download hieronder het Excel-bestand.")
        st.download_button(
            "Download rooster.xlsx",
            data=xlsx.getvalue(),
            file_name="rooster.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            on_click=_mark_downloaded,  # <- zet vlag
        )

        # Auto-reset zodra de download gestart is (werkt prettig op iPhone)
        if st.session_state.get("_downloaded"):
            st.toast("Download gestart — app wordt gereset…", icon="✅")
            st.session_state.clear()
            _safe_rerun()

    except Exception as e:
        st.error(f"Er ging iets mis: {e}")
