#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import warnings
from typing import List, Dict, Tuple, Optional

import pandas as pd
import requests
import streamlit as st
from datetime import datetime
from urllib.parse import quote, urlparse, parse_qs, urlencode, urlunparse
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# ===== tijdzone helpers =====
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo

def now_naive_in_tz(tz_str: str) -> pd.Timestamp:
    return pd.Timestamp(datetime.now(ZoneInfo(tz_str))).tz_localize(None)

def now_aware_in_tz(tz_str: str) -> pd.Timestamp:
    return pd.Timestamp(datetime.now(ZoneInfo(tz_str)))

# Onderdruk macOS LibreSSL warning van urllib3
warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL.*",
    category=Warning,
    module=r"urllib3\.__init__"
)

# === VASTE INSTELLINGEN ===
DEFAULT_CLIENT_ID = "K662D1WXrt"
TZ = "Europe/Amsterdam"
DAYS_AHEAD = 60
WEEK_OFFSET = -1
FIELDS = "naam,datumvanaf,datumtot,tijdvanaf,tijdtot,lokatie,heledag"

BAR_CODES = ["445", "701", "741"]
CK_CODES  = ["442"]
WEEK_LABEL = "short"          # of "iso"
SAT_ONLY_CK = True            # CommissieKamer alleen zaterdag

# Wedstrijden (programma)
PROGRAM_DAYS_AHEAD = 60
PROGRAM_FIELDS = ("wedstrijddatum,wedstrijdnummer,thuisteamclubrelatiecode,"
                  "uitteamclubrelatiecode,thuisteam,uitteam,competitiesoort,aanvangstijd")
CKC_CLUBRELATIECODE = "BBDZ08H"

DAYS_NL = ["Maandag","Dinsdag","Woensdag","Donderdag","Vrijdag","Zaterdag","Zondag"]
DAY_COLORS = {
    "Maandag":"FFDDEBF7","Dinsdag":"FFE2EFDA","Woensdag":"FFFFF2CC",
    "Donderdag":"FFFCE4D6","Vrijdag":"FFE7E6E6","Zaterdag":"FFE4DFEC","Zondag":"FFF8CBAD"
}

# Shifts per dag: (Tijd-van, Tijd-tot)
DEFAULT_SLOTS: Dict[str, List[Tuple[str, str]]] = {
    "Maandag":   [("18:00","19:00"), ("19:00","20:00")],
    "Dinsdag":   [("18:00","22:00")],
    "Woensdag":  [("17:00","18:00"), ("18:00","19:00"), ("19:00","22:00")],
    "Donderdag": [("18:00","22:00")],
    "Vrijdag":   [("18:00","20:30"), ("20:30","23:00")],
    "Zaterdag":  [("07:30","10:00"), ("10:00","12:30"),
                  ("12:30","15:00"), ("15:00","17:30"),
                  ("17:30","20:00"), ("20:00","22:30")],
    "Zondag":    [("10:00","12:30"), ("12:30","15:00")],
}

# Dropbox handmatige input
DROPBOX_INPUT_URL = "https://www.dropbox.com/scl/fi/ukcs87y9h1j27uyzcotig/rooster_input.txt?rlkey=fx0ayzshabo7zikun620m61hh&st=vtrlzr8k&dl=0"

# ---------- Helpers ----------
def month_short_nl(m:int) -> str:
    return ["jan","feb","mrt","apr","mei","jun","jul","aug","sept","okt","nov","dec"][m-1]

def build_urls(taskcodes: List[str], days: int, client_id: str,
               weekoffset: int = -1, fields: Optional[str] = FIELDS) -> List[str]:
    base = "https://data.sportlink.com/vrijwilligers"
    urls = []
    for code in taskcodes:
        url = (f"{base}?vrijwilligerstaakcode={code}"
               f"&aantaldagen={int(days)}&client_id={client_id}&weekoffset={int(weekoffset)}")
        if fields:
            url += f"&fields={quote(fields)}"
        urls.append(url)
    return urls

def build_program_url(days: int, client_id: str, fields: str = PROGRAM_FIELDS,
                      eigenwedstrijden: str = "JA", thuis: str = "JA", uit: str = "NEE",
                      gebruiklokaleteamgegevens: str = "NEE") -> str:
    base = "https://data.sportlink.com/programma"
    url = (f"{base}?aantaldagen={int(days)}&client_id={client_id}"
           f"&eigenwedstrijden={eigenwedstrijden}&thuis={thuis}&uit={uit}"
           f"&gebruiklokaleteamgegevens={gebruiklokaleteamgegevens}")
    if fields:
        url += f"&fields={quote(fields)}"
    return url

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
                import time as _t; _t.sleep(backoff_factor * (2 ** (attempt - 1)))
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
    dat = dat.dt.tz_convert(tz_str).dt.tz_localize(None)
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
    if "Datum" not in df.columns:
        return df.iloc[0:0].copy()
    if not pd.api.types.is_datetime64_any_dtype(df["Datum"]):
        df = df.copy()
        df["Datum"] = pd.to_datetime(df["Datum"], errors="coerce")
    df = df.dropna(subset=["Datum"]).copy()
    if df.empty:
        return df
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

# ===== matrix met subregels (intern) =====
REGELS = ["Handmatig", "Wedstrijden", "Namen"]

def build_empty_matrix(slots: Dict[str, List[Tuple[str,str]]],
                       tz_str: str,
                       days_subset=None,
                       horizon_weeks_if_empty=4,
                       week_label_style: str="short",
                       weeks_pairs=None, week_mondays=None) -> pd.DataFrame:
    if weeks_pairs is None or week_mondays is None:
        raise ValueError("weeks_pairs en week_mondays zijn verplicht")

    def week_label(pair: Tuple[int,int]) -> str:
        y, w = pair
        return f"{y}-W{w:02d}" if week_label_style == "iso" else f"Week {w}"

    days_to_use = DAYS_NL if days_subset is None else [d for d in DAYS_NL if d in days_subset]
    rows = []
    for d in days_to_use:
        rows.append((d, "", "", ""))  # dag-header
        for (van, tot) in slots.get(d, []):
            for r in REGELS:
                rows.append((d, van, tot, r))

    matrix = pd.DataFrame(
        "",
        index=pd.MultiIndex.from_tuples(rows, names=["Dag","Tijd-van","Tijd-tot","Regel"]),
        columns=[week_label(p) for p in weeks_pairs]
    )

    # Kolom-headers met datum
    for p in weeks_pairs:
        mon = week_mondays[p]; col = week_label(p)
        for d in days_to_use:
            day_date = (mon + pd.Timedelta(days=DAYS_NL.index(d))).strftime("%d-%b")
            matrix.loc[(d,"","", ""), col] = f"{d} ({day_date})"

    return matrix

def _hhmm_to_minutes(hhmm: str) -> int:
    try:
        h, m = hhmm.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return -1

def fill_names(matrix: pd.DataFrame, df: pd.DataFrame,
               slots: Dict[str, List[Tuple[str,str]]],
               week_label_style: str):
    for _, r in df.iterrows():
        d = r["Dag"]; t_from = r["Tijd vanaf"]; t_to = r["Tijd tot"]
        w = int(r["Week"]); y = int(r["ISO_Year"])
        if pd.isna(d) or pd.isna(w) or not t_from:
            continue
        if not t_to:
            for (van, tot) in slots.get(d, []):
                if van == t_from:
                    t_to = tot; break
        col = (f"{y}-W{w:02d}" if week_label_style=="iso" else f"Week {w}")
        if (d in slots) and ((t_from, t_to) in slots.get(d, [])) and (col in matrix.columns):
            cur = matrix.loc[(d, t_from, t_to, "Namen"), col]
            name = str(r["Naam"]) if pd.notna(r["Naam"]) else ""
            matrix.loc[(d, t_from, t_to, "Namen"), col] = (cur + "\n" + name) if cur else name

def fill_manual(matrix: pd.DataFrame, annotations, slots: Dict[str, List[Tuple[str,str]]],
                week_label_style: str):
    def week_label(y, w): return f"{y}-W{w:02d}" if week_label_style=="iso" else f"Week {w}"
    for a in annotations:
        label = week_label(a["iso_year"], a["iso_week"])
        tot = None
        for (v, t) in slots.get(a["day"], []):
            if v == a["time_from"]:
                tot = t; break
        if tot is None or label not in matrix.columns:
            continue
        key = (a["day"], a["time_from"], tot, "Handmatig")
        cur = matrix.loc[key, label]
        matrix.loc[key, label] = (cur + "\n" + a["text"]) if cur else a["text"]

def build_match_index_for_overlap(df_program: pd.DataFrame) -> Dict[tuple, List[Tuple[int, str]]]:
    idx: Dict[tuple, List[Tuple[int, str]]] = {}
    for _, r in df_program.iterrows():
        y, w, d = int(r["ISO_Year"]), int(r["Week"]), r["Dag"]
        try:
            h, m = str(r["Tijd"]).split(":"); tmin = int(h)*60+int(m)
        except Exception:
            continue
        team = str(r["HomeTeam"]).strip()
        if team:
            idx.setdefault((y, w, d), []).append((tmin, team))
    return idx

def fill_matches(matrix: pd.DataFrame, match_index,
                 week_label_style: str, slots: Dict[str, List[Tuple[str,str]]]):
    cols = list(matrix.columns)
    for (d, van, tot, regel) in matrix.index:
        if not (van and tot and regel == "Wedstrijden"):
            continue
        v_from = _hhmm_to_minutes(van); v_to = _hhmm_to_minutes(tot)
        if v_from < 0 or v_to <= v_from:
            continue
        for label in cols:
            if week_label_style == "iso":
                parts = label.split("-W"); y, w = int(parts[0]), int(parts[1])
            else:
                try: w = int(label.split()[1])
                except Exception: continue
                y = now_naive_in_tz(TZ).isocalendar().year
            teams = []
            for tmin, team in match_index.get((y, w, d), []):
                if v_from <= tmin < v_to:
                    teams.append(team)
            if teams:
                matrix.loc[(d, van, tot, "Wedstrijden"), label] = ", ".join(teams)

def prune_empty_subrows(matrix: pd.DataFrame) -> pd.DataFrame:
    # behoud headers (van==""==tot==""==regel=="")
    keep = []
    for idx in matrix.index:
        d, van, tot, regel = idx
        if not van and not tot and not regel:
            keep.append(True); continue
        row = matrix.loc[idx]
        has_content = any(bool(str(v)) for v in row.values)
        keep.append(has_content)
    return matrix[keep]

# ===== formatter =====
def format_sheet(ws, matrix: pd.DataFrame, slots: Dict[str, List[Tuple[str,str]]], tz_str: str):
    thin = Side(style="thin", color="FFAAAAAA")
    thick = Side(style="thick", color="FF000000")
    bold = Font(bold=True)
    wrap = Alignment(wrap_text=True, vertical="top")
    center = Alignment(horizontal="center", vertical="center")

    first_week_col_idx = 4  # A:Dag, B:Tijd-van, C:Tijd-tot, D: (Regel is weggehaald), Eerder 5 -> nu 4
    last_col_idx = first_week_col_idx + ws.max_column - 3  # dynamisch na delete

    # Bepaal laatste rij per dag voor dikke lijn
    day_last_row = {}
    last_seen = {}
    for r_idx, (d, van, tot, regel) in enumerate(matrix.index, start=2):  # header is rij 1
        last_seen[d] = r_idx
        if van and regel in ("Handmatig","Wedstrijden","Namen"):
            day_last_row[d] = r_idx
    # Opmaak per rij/kolom
    for r_idx, (d, van, tot, regel) in enumerate(matrix.index, start=2):
        is_header = (van == "" and tot == "" and regel == "")
        for c_idx in range(1, ws.max_column+1):
            cell = ws.cell(row=r_idx, column=c_idx)
            cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)
            if is_header:
                fill = PatternFill(start_color=DAY_COLORS.get(d, "FFFFFFFF"),
                                   end_color=DAY_COLORS.get(d, "FFFFFFFF"),
                                   fill_type="solid")
                cell.fill = fill
                cell.font = bold
                cell.alignment = center
            else:
                # Regel is intern; kleur op basis van regel
                if regel in ("Handmatig","Wedstrijden"):
                    cell.font = Font(color="FFCC0000")
                else:
                    cell.font = Font(color="FF000000")
                cell.alignment = wrap

        if r_idx in day_last_row.values():
            for c_idx in range(1, ws.max_column+1):
                cell = ws.cell(row=r_idx, column=c_idx)
                cell.border = Border(left=cell.border.left, right=cell.border.right,
                                     top=cell.border.top, bottom=thick)

    # Dikke verticale scheiding tussen weekkolommen
    for j in range(first_week_col_idx, ws.max_column+1):
        for r in range(1, ws.max_row+1):
            cell = ws.cell(row=r, column=j)
            cell.border = Border(left=thick, right=cell.border.right,
                                 top=cell.border.top, bottom=cell.border.bottom)

    # Kolombreedtes
    ws.column_dimensions['A'].width = 12
    ws.column_dimensions['B'].width = 9
    ws.column_dimensions['C'].width = 9
    for col_cells in ws.iter_cols(min_col=first_week_col_idx, max_col=ws.max_column):
        max_len = 14
        for cell in col_cells:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_cells[0].column_letter].width = min(max(int(max_len*0.75)+2, 10), 24)

    # Timestamp in A1
    now = now_naive_in_tz(tz_str)
    stamp = f"{now.day} {month_short_nl(now.month)} {now.strftime('%H:%M')}"
    a1 = ws.cell(row=1, column=1); a1.value = stamp
    try: a1.font = Font(italic=True, color="FF666666")
    except Exception: pass

    ws.freeze_panes = "D2"  # tot en met Tijd-tot + header

# ===== Wedstrijden normaliseren =====
def _strip_ckc_prefix(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.strip()
    up = s.upper()
    if up.startswith("CKC JO") or up.startswith("CKC MO") or up.startswith("CKC O") or up.startswith("CKC VR"):
        return s[4:].lstrip()
    return s

def normalize_program(data, tz_str: str) -> pd.DataFrame:
    df = pd.DataFrame(data)
    empty = pd.DataFrame(columns=["Datum","Dag","Tijd","ISO_Year","Week","HomeTeam"])
    if df.empty:
        return empty

    c_date = _pick(df.columns, ["wedstrijddatum"])
    c_home = _pick(df.columns, ["thuisteam"])
    c_home_code = _pick(df.columns, ["thuisteamclubrelatiecode"])
    for col in (c_date, c_home, c_home_code):
        if col is None or col not in df.columns:
            return empty

    dt = pd.to_datetime(df[c_date].astype(str).str.strip(), errors="coerce", utc=True)
    mask_ok = dt.notna()
    if not mask_ok.any():
        return empty
    dt_local_naive = dt.dt.tz_convert(ZoneInfo(tz_str)).dt.tz_localize(None)

    df = df.loc[mask_ok].copy()
    df["Datum"] = dt_local_naive.astype("datetime64[ns]")

    df = df[df[c_home_code].astype(str) == CKC_CLUBRELATIECODE].copy()
    if df.empty:
        return empty

    df["Dag"] = df["Datum"].dt.weekday.map(lambda i: DAYS_NL[i])
    df["Tijd"] = df["Datum"].dt.strftime("%H:%M")
    iso = df["Datum"].dt.isocalendar()
    df["ISO_Year"] = iso.year.astype(int)
    df["Week"] = iso.week.astype(int)
    df["HomeTeam"] = df[c_home].astype(str).map(_strip_ckc_prefix)
    return df[["Datum","Dag","Tijd","ISO_Year","Week","HomeTeam"]]

# ===== Handmatige input (.txt) =====
def parse_manual_text(text: str):
    entries = []
    if not text:
        return entries
    now_aw = now_aware_in_tz(TZ)
    monday_aw = (now_aw - pd.Timedelta(days=int(now_aw.weekday()))).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"): continue
        parts = s.split()
        if len(parts) < 3: continue
        date_str, time_str = parts[0], parts[1]
        txt = " ".join(parts[2:]).strip()
        try:
            dt_aw = pd.Timestamp(f"{date_str} {time_str}", tz=ZoneInfo(TZ))
        except Exception:
            continue
        if dt_aw < monday_aw: continue
        day_name = DAYS_NL[int(dt_aw.weekday())]
        starts = [a for a, b in DEFAULT_SLOTS.get(day_name, [])]
        if time_str not in starts: continue
        iso = dt_aw.isocalendar()
        entries.append({
            "date": dt_aw.tz_convert(None),
            "time_from": time_str,
            "text": txt,
            "iso_year": int(iso.year),
            "iso_week": int(iso.week),
            "day": day_name,
        })
    return entries

# -------- Dropbox helper --------
def _ensure_dropbox_direct(url: str) -> str:
    if not url or "dropbox.com" not in url:
        return url
    try:
        pr = urlparse(url)
        qs = parse_qs(pr.query)
        qs["dl"] = ["1"]
        new_query = urlencode({k: v[0] for k, v in qs.items()})
        return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_query, pr.fragment))
    except Exception:
        return url.replace("dl=0", "dl=1") if "dl=" in url else (url + ("&" if "?" in url else "?") + "dl=1")

def read_manual_text_from_dropbox(timeout: int = 30) -> str:
    direct = _ensure_dropbox_direct(DROPBOX_INPUT_URL)
    r = requests.get(direct, timeout=timeout)
    r.raise_for_status()
    return r.text

# ===== Excel bouwen =====
def make_excel(df_bar, df_ck, annotations, use_matches=True):
    # Weekrange bepalen: uit data + huidige week + annotaties
    def compute_weeks(df_list):
        pairs = set()
        for df in df_list:
            if df is None or df.empty: continue
            for y, w in zip(df["ISO_Year"], df["Week"]):
                pairs.add((int(y), int(w)))
        now = now_naive_in_tz(TZ); iso = now.isocalendar()
        pairs.add((int(iso.year), int(iso.week)))
        pairs |= {(a["iso_year"], a["iso_week"]) for a in annotations}
        pairs = sorted(pairs)
        wmondays = {p: pd.Timestamp.fromisocalendar(p[0], p[1], 1) for p in pairs}
        return pairs, wmondays

    weeks_pairs, week_mondays = compute_weeks([df_bar, df_ck])

    # Lege matrixen opzetten
    matrix_bar = build_empty_matrix(DEFAULT_SLOTS, TZ, None, 4, WEEK_LABEL, weeks_pairs, week_mondays)
    days_subset_ck = ["Zaterdag"] if SAT_ONLY_CK else None
    matrix_ck  = build_empty_matrix(DEFAULT_SLOTS, TZ, days_subset_ck, 4, WEEK_LABEL, weeks_pairs, week_mondays)

    # Vullen: namen
    fill_names(matrix_bar, df_bar, DEFAULT_SLOTS, WEEK_LABEL)
    fill_names(matrix_ck,  df_ck,  DEFAULT_SLOTS, WEEK_LABEL)

    # Vullen: handmatig
    fill_manual(matrix_bar, annotations, DEFAULT_SLOTS, WEEK_LABEL)
    fill_manual(matrix_ck,  annotations, DEFAULT_SLOTS, WEEK_LABEL)

    # Vullen: wedstrijden
    if use_matches:
        # bouw match index
        program_url = build_program_url(PROGRAM_DAYS_AHEAD, DEFAULT_CLIENT_ID, PROGRAM_FIELDS,
                                        eigenwedstrijden="JA", thuis="JA", uit="NEE",
                                        gebruiklokaleteamgegevens="NEE")
        program_json = http_get_json(program_url)
        df_program = normalize_program(program_json, TZ)
        df_program = filter_from_current_week(df_program, TZ)
        match_index = build_match_index_for_overlap(df_program)
        fill_matches(matrix_bar, match_index, WEEK_LABEL, DEFAULT_SLOTS)
        fill_matches(matrix_ck,  match_index, WEEK_LABEL, DEFAULT_SLOTS)

    # Prune: verwijder subregels die volledig leeg zijn
    matrix_bar = prune_empty_subrows(matrix_bar)
    matrix_ck  = prune_empty_subrows(matrix_ck)

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        # BarRooster
        matrix_bar.to_excel(writer, sheet_name="BarRooster")  # index -> kolommen A-D (Regel = kolom D)
        ws_bar = writer.sheets["BarRooster"]
        ws_bar.delete_cols(4)  # Regel-kolom verwijderen
        format_sheet(ws_bar, matrix_bar, DEFAULT_SLOTS, TZ)

        # CommissieKamer
        matrix_ck.to_excel(writer, sheet_name="CommissieKamer")
        ws_ck = writer.sheets["CommissieKamer"]
        ws_ck.delete_cols(4)
        format_sheet(ws_ck, matrix_ck, DEFAULT_SLOTS, TZ)

    bio.seek(0)
    return bio

# =========================
# UI (simpel & mobiel)
# =========================
st.set_page_config(page_title="CKC Rooster generator", page_icon="🗓️", layout="centered")
st.markdown("<h1 style='text-align:center;margin-bottom:0'>CKC Rooster generator</h1>", unsafe_allow_html=True)
st.markdown("<h5 style='text-align:center;margin-top:0.25rem;color:#666'>versie 2.5</h5>", unsafe_allow_html=True)
st.caption("Sportlink → Excel · vaste instellingen (Europe/Amsterdam), weekoffset=-1, gefilterd vanaf huidige week")

use_dropbox = st.checkbox("Handmatige input via Dropbox meenemen")
use_matches = st.checkbox("Wedstrijdinfo toevoegen", value=True)

if st.button("Genereer rooster", use_container_width=True):
    try:
        with st.spinner("Ophalen en bouwen…"):
            # Vrijwilligersdata
            urls_bar = build_urls(BAR_CODES, DAYS_AHEAD, DEFAULT_CLIENT_ID, weekoffset=WEEK_OFFSET, fields=FIELDS)
            urls_ck  = build_urls(CK_CODES,  DAYS_AHEAD, DEFAULT_CLIENT_ID, weekoffset=WEEK_OFFSET, fields=FIELDS)
            all_bar = sum([http_get_json(u) for u in urls_bar], [])
            all_ck  = sum([http_get_json(u) for u in urls_ck],  [])
            df_bar = filter_from_current_week(normalize_dataframe(all_bar, TZ), TZ)
            df_ck  = filter_from_current_week(normalize_dataframe(all_ck,  TZ), TZ)

            # Handmatige input
            manual_text = ""
            if use_dropbox:
                # Dropbox direct-download
                pr = urlparse(DROPBOX_INPUT_URL)
                qs = parse_qs(pr.query); qs["dl"] = ["1"]
                direct_url = urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, urlencode({k:v[0] for k,v in qs.items()}), pr.fragment))
                manual_text = requests.get(direct_url, timeout=30).text
            annotations = parse_manual_text(manual_text)

            # Excel bouwen (met/zonder wedstrijden)
            xlsx = make_excel(df_bar, df_ck, annotations, use_matches=use_matches)

        st.success("Klaar! Download hieronder het Excel-bestand.")
        st.download_button(
            "⬇️ Download rooster.xlsx",
            data=xlsx.getvalue(),
            file_name="rooster.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    except requests.HTTPError as e:
        st.error(f"Fout bij ophalen gegevens: {e}")
    except Exception as e:
        st.error(f"Er ging iets mis: {e}")