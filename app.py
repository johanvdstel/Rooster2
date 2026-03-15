#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CKC Rooster Generator
versie 3.0
Sneller / stabieler / minder code
"""

import io
import time
import warnings
from typing import List, Dict, Tuple

import pandas as pd
import requests
import streamlit as st
from datetime import datetime
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# ========= instellingen =========

__version__ = "3.0"

TZ = "Europe/Amsterdam"
CLIENT_ID = "K662D1WXrt"

BAR_CODES = ["701", "741", "761"]
CK_CODES = ["442"]

DAYS_AHEAD = 60
PROGRAM_DAYS_AHEAD = 60
WEEK_OFFSET = -1

DROPBOX_INPUT_URL = "https://www.dropbox.com/scl/fi/ukcs87y9h1j27uyzcotig/rooster_input.txt?dl=1"

# ========= session =========

session = requests.Session()

# ========= dagen =========

DAYS_NL = [
    "Maandag","Dinsdag","Woensdag",
    "Donderdag","Vrijdag","Zaterdag","Zondag"
]

# ========= slots =========

SLOTS = {
    "Maandag":[("18:00","19:00"),("19:00","20:00"),("20:00","22:30")],
    "Dinsdag":[("18:00","19:00"),("19:00","20:00"),("20:00","22:30")],
    "Woensdag":[("17:00","18:00"),("18:00","19:00"),("19:00","20:00"),("20:00","22:30")],
    "Donderdag":[("18:00","19:00"),("19:00","20:00"),("20:00","22:30")],
    "Vrijdag":[("17:00","20:00"),("20:00","22:30")],
    "Zaterdag":[("07:30","10:00"),("10:00","12:30"),("12:30","15:00"),("15:00","17:30"),("17:30","20:00"),("20:00","22:30")],
    "Zondag":[("10:00","12:30"),("12:30","15:00")]
}

# ========= helpers =========

def http_get_json(url: str):

    for attempt in range(3):

        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()

            data = r.json()

            if isinstance(data, dict) and "items" in data:
                data = data["items"]

            if not isinstance(data, list):
                return []

            return data

        except Exception:

            if attempt < 2:
                time.sleep(1)
            else:
                return []

# ========= parallel fetch =========

@st.cache_data(ttl=300)
def fetch_all(urls: List[str]) -> List[dict]:

    with ThreadPoolExecutor(max_workers=8) as exe:
        results = list(exe.map(http_get_json, urls))

    safe = [r if isinstance(r,list) else [] for r in results]

    return list(chain.from_iterable(safe))

# ========= sportlink urls =========

def build_urls(codes):

    base = "https://data.sportlink.com/vrijwilligers"

    urls = []

    for code in codes:

        url = (
            f"{base}?vrijwilligerstaakcode={code}"
            f"&aantaldagen={DAYS_AHEAD}"
            f"&weekoffset={WEEK_OFFSET}"
            f"&client_id={CLIENT_ID}"
        )

        urls.append(url)

    return urls

# ========= dataframe =========

def normalize(df):

    if not df:
        return pd.DataFrame()

    df = pd.DataFrame(df)

    cols = {c.lower():c for c in df.columns}

    name = cols.get("naam")
    dv = cols.get("datumvanaf")
    tv = cols.get("tijdvanaf")
    tt = cols.get("tijdtot")

    out = pd.DataFrame()

    out["Naam"] = df[name]
    out["Datum"] = pd.to_datetime(df[dv], errors="coerce")
    out["Start"] = df[tv]
    out["Eind"] = df[tt]

    out = out.dropna(subset=["Datum"])

    out["Dag"] = out["Datum"].dt.weekday.map(lambda i:DAYS_NL[i])
    out["Week"] = out["Datum"].dt.isocalendar().week
    out["ISO_Year"] = out["Datum"].dt.isocalendar().year

    out = out.drop_duplicates()

    return out

# ========= matrix =========

def build_matrix(weeks):

    rows=[]

    for d in DAYS_NL:

        rows.append((d,"",""))

        for s in SLOTS[d]:

            rows.append((d,s[0],s[1]))

    index=pd.MultiIndex.from_tuples(rows,names=["Dag","Van","Tot"])

    cols=[f"Week {w}" for w in weeks]

    return pd.DataFrame("",index=index,columns=cols)

# ========= vullen =========

def fill_names(matrix,df):

    for _,r in df.iterrows():

        d=r["Dag"]
        w=r["Week"]

        col=f"Week {w}"

        if col not in matrix.columns:
            continue

        name=str(r["Naam"]).strip()

        start=r["Start"]

        for s in SLOTS.get(d,[]):

            if s[0]==start:

                cur=matrix.loc[(d,s[0],s[1]),col]

                if cur:
                    cur=cur+"\n"+name
                else:
                    cur=name

                matrix.loc[(d,s[0],s[1]),col]=cur

# ========= excel =========

def make_excel(bar,ck):

    bio=io.BytesIO()

    with pd.ExcelWriter(bio,engine="openpyxl") as writer:

        bar.to_excel(writer,sheet_name="BarRooster")
        ck.to_excel(writer,sheet_name="CommissieKamer")

    bio.seek(0)

    return bio

# ========= UI =========

st.set_page_config(page_title="CKC rooster generator",page_icon="🗓️")

st.title("CKC Rooster generator")

use_dropbox=st.checkbox("Handmatige input via Dropbox")

if st.button("Genereer rooster"):

    with st.spinner("Sportlink ophalen..."):

        urls_bar=build_urls(BAR_CODES)
        urls_ck=build_urls(CK_CODES)

        data_bar=fetch_all(urls_bar)
        data_ck=fetch_all(urls_ck)

        df_bar=normalize(data_bar)
        df_ck=normalize(data_ck)

        weeks=sorted(set(df_bar["Week"]).union(df_ck["Week"]))

        matrix_bar=build_matrix(weeks)
        matrix_ck=build_matrix(weeks)

        fill_names(matrix_bar,df_bar)
        fill_names(matrix_ck,df_ck)

        xlsx=make_excel(matrix_bar,matrix_ck)

    st.success("Klaar!")

    st.download_button(
        "Download rooster.xlsx",
        data=xlsx,
        file_name="rooster.xlsx"
    )