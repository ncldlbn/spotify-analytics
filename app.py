import streamlit as st
import pandas as pd
import plotly.express as px
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime, timedelta
import json, os, io

st.set_page_config(layout="wide")

# ======================= CACHE PERSISTENTE =======================
ENRICHED_CACHE_FILE = "enriched_cache.json"

def load_enriched_cache():
    if os.path.exists(ENRICHED_CACHE_FILE):
        with open(ENRICHED_CACHE_FILE, "r") as f:
            return json.load(f)
    return {"artists": {}, "albums": {}}

def save_enriched_cache(cache):
    with open(ENRICHED_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

if "enriched_cache" not in st.session_state:
    st.session_state.enriched_cache = load_enriched_cache()

# ======================= SPOTIFY CLIENT =======================
try:
    client_id = st.secrets["SPOTIFY_CLIENT_ID"]
    client_secret = st.secrets["SPOTIFY_CLIENT_SECRET"]
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret))
except Exception:
    st.error("Errore di configurazione Spotify API. Verifica le credenziali in st.secrets.")
    st.stop()

# ======================= SIDEBAR — FILE UPLOAD =======================
st.sidebar.title("Spotify History Analytics")
uploaded_files = st.sidebar.file_uploader("", type="json", accept_multiple_files=True)

if not uploaded_files:
    st.sidebar.markdown(
        'Download your extended history file from <a href="https://www.spotify.com/it/account/privacy/?flow_ctx=59b43f29-051e-45c1-ba9f-fc4d09835f73%3A1772554735" target="_blank">here</a>.',
        unsafe_allow_html=True
    )
    st.stop()

# ======================= CARICAMENTO BASE =======================
@st.cache_data(show_spinner=False)
def load_base_df(file_ids):
    dfs = []
    for _, content in file_ids:
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        dfs.append(pd.read_json(io.StringIO(content)))
    df = pd.concat(dfs, ignore_index=True)
    df = df[df["master_metadata_track_name"].notna()].copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["data"] = df["ts"].dt.date
    df["ora"] = df["ts"].dt.time
    df["sec_played"] = (df["ms_played"] / 1000).round(1)
    df = df.rename(columns={
        "master_metadata_track_name": "track_name",
        "master_metadata_album_artist_name": "artist_name",
        "master_metadata_album_album_name": "album_name",
    })
    if "skipped" in df.columns:
        df["skipped"] = df["skipped"].fillna(False).astype(bool)
    else:
        df["skipped"] = False
    df = df[df["sec_played"] > 30]
    return df[["ts","data","ora","sec_played","track_name","album_name",
               "artist_name","skipped"]].reset_index(drop=True)

file_ids = tuple((f.name, f.read()) for f in uploaded_files)
for f in uploaded_files:
    f.seek(0)

with st.spinner("Uploading JSON file..."):
    base_df = load_base_df(file_ids)

# ======================= ENRICHMENT DA SPOTIFY =======================
def enrich_artist(artist_name):
    cache = st.session_state.enriched_cache["artists"]
    if artist_name in cache:
        return cache[artist_name]
    try:
        result = sp.search(q=f'artist:"{artist_name}"', type="artist", limit=1)
        items = result["artists"]["items"]
        if items:
            genres = items[0].get("genres", [])
            images = items[0].get("images", [])
            data = {"genre": genres[0] if genres else "",
                    "url_artist_img": images[0]["url"] if images else ""}
        else:
            data = {"genre": "", "url_artist_img": ""}
    except Exception:
        data = {"genre": "", "url_artist_img": ""}
    cache[artist_name] = data
    return data

def enrich_album(artist_name, album_name):
    key = f"{artist_name}||{album_name}"
    cache = st.session_state.enriched_cache["albums"]
    if key in cache:
        return cache[key]
    try:
        result = sp.search(q=f'album:"{album_name}" artist:"{artist_name}"', type="album", limit=1)
        items = result["albums"]["items"]
        if items:
            images = items[0].get("images", [])
            release = items[0].get("release_date", "")
            data = {"url_album_img": images[0]["url"] if images else "",
                    "year_published": release[:4] if release else ""}
        else:
            data = {"url_album_img": "", "year_published": ""}
    except Exception:
        data = {"url_album_img": "", "year_published": ""}
    cache[key] = data
    return data

cache = st.session_state.enriched_cache
needs_enrichment = (
    any(a not in cache["artists"] for a in base_df["artist_name"].dropna().unique()) or
    any(f"{r.artist_name}||{r.album_name}" not in cache["albums"]
        for r in base_df[["artist_name","album_name"]].dropna().drop_duplicates().itertuples()))

if needs_enrichment:
    with st.status("Reading data... at least some minutes required", expanded=True):
        artists_to_fetch = [a for a in base_df["artist_name"].dropna().unique()
                            if a not in cache["artists"]]
        if artists_to_fetch:
            bar = st.progress(0, text="Retrieving artist info...")
            for i, artist in enumerate(artists_to_fetch):
                bar.progress((i+1)/len(artists_to_fetch),
                             text=f"{i+1}/{len(artists_to_fetch)}: {artist}")
                enrich_artist(artist)
            bar.empty()

        album_keys = set(f"{r.artist_name}||{r.album_name}"
                         for r in base_df[["artist_name","album_name"]].dropna().drop_duplicates().itertuples())
        albums_to_fetch = [k for k in album_keys if k not in cache["albums"]]
        if albums_to_fetch:
            bar = st.progress(0, text="Retrieving albums data...")
            for i, key in enumerate(albums_to_fetch):
                bar.progress((i+1)/len(albums_to_fetch), text=f"Album {i+1}/{len(albums_to_fetch)}")
                artist, album = key.split("||", 1)
                enrich_album(artist, album)
            bar.empty()

        save_enriched_cache(cache)
    st.success("Done!")
    st.rerun()

def build_enriched_df(df):
    c = st.session_state.enriched_cache
    artist_meta = pd.DataFrame([
        {"artist_name": k, "genre": v.get("genre",""), "url_artist_img": v.get("url_artist_img","")}
        for k, v in c["artists"].items()])
    album_rows = []
    for k, v in c["albums"].items():
        artist, album = k.split("||", 1)
        album_rows.append({"artist_name": artist, "album_name": album,
                           "url_album_img": v.get("url_album_img",""),
                           "year_published": v.get("year_published","")})
    album_meta = pd.DataFrame(album_rows)
    enriched = df.merge(artist_meta, on="artist_name", how="left")
    enriched = enriched.merge(album_meta, on=["artist_name","album_name"], how="left")
    cols = ["data","ora","sec_played","track_name","album_name","artist_name",
            "skipped","genre","year_published","url_artist_img","url_album_img"]
    return enriched[[c for c in cols if c in enriched.columns]]

df = build_enriched_df(base_df)
df["ts"]    = pd.to_datetime(df["data"].astype(str), utc=True)
df["hours"] = df["sec_played"] / 3600
df["year"]  = df["ts"].dt.year

# ======================= SIDEBAR — PERIODO =======================
it_months = {1:"gennaio",2:"febbraio",3:"marzo",4:"aprile",5:"maggio",6:"giugno",
             7:"luglio",8:"agosto",9:"settembre",10:"ottobre",11:"novembre",12:"dicembre"}

period_option = st.sidebar.selectbox("Seleziona periodo", [
    "Tutto il periodo","Ultimo mese","Ultimi 3 mesi","Ultimi 6 mesi",
    "Ultimi 12 mesi","Anno specifico","Mese specifico","Periodo personalizzato"])

year_selected = selected_m_str = start_date_custom = end_date_custom = None
if period_option == "Anno specifico":
    year_selected = st.sidebar.selectbox("Anno", sorted(df["year"].unique(), reverse=True))
elif period_option == "Mese specifico":
    months_options = df.sort_values("ts", ascending=False)["ts"].dt.to_period("M").unique()
    formatted_options = [f"{m.year} {it_months[m.month]}" for m in months_options]
    selected_m_str = st.sidebar.selectbox("Seleziona mese", formatted_options)
elif period_option == "Periodo personalizzato":
    start_date_custom = st.sidebar.date_input("Data inizio", df["ts"].min().date())
    end_date_custom   = st.sidebar.date_input("Data fine",   df["ts"].max().date())

today = df["ts"].max()
if period_option == "Tutto il periodo":
    start_date, end_date = df["ts"].min(), today
elif period_option == "Ultimo mese":
    start_date, end_date = today - timedelta(days=30), today
elif period_option == "Ultimi 3 mesi":
    start_date, end_date = today - timedelta(days=90), today
elif period_option == "Ultimi 6 mesi":
    start_date, end_date = today - timedelta(days=180), today
elif period_option == "Ultimi 12 mesi":
    start_date, end_date = today - timedelta(days=365), today
elif period_option == "Anno specifico" and year_selected:
    start_date = pd.Timestamp(datetime(year_selected, 1, 1), tz="UTC")
    end_date   = pd.Timestamp(datetime(year_selected, 12, 31, 23, 59, 59), tz="UTC")
elif period_option == "Mese specifico" and selected_m_str:
    sel_year  = int(selected_m_str.split()[0])
    sel_month = {v:k for k,v in it_months.items()}[selected_m_str.split()[1]]
    start_date = pd.Timestamp(datetime(sel_year, sel_month, 1), tz="UTC")
    end_date   = (pd.Timestamp(datetime(sel_year, 12, 31, 23, 59, 59), tz="UTC") if sel_month == 12
                  else pd.Timestamp(datetime(sel_year, sel_month+1, 1), tz="UTC") - timedelta(seconds=1))
elif period_option == "Periodo personalizzato" and start_date_custom and end_date_custom:
    start_date = pd.Timestamp(start_date_custom, tz="UTC")
    end_date   = pd.Timestamp(end_date_custom,   tz="UTC")
else:
    start_date, end_date = df["ts"].min(), today

filtered = df[(df["ts"] >= start_date) & (df["ts"] <= end_date)]

# ======================= SIDEBAR — FILTRO ARTISTA =======================
artist_options  = ["Tutti"] + sorted(filtered["artist_name"].dropna().unique().tolist())
selected_artist = st.sidebar.selectbox("Filtra per artista", artist_options)

filtered_view = filtered if selected_artist == "Tutti" else filtered[filtered["artist_name"] == selected_artist]
history_df    = df[df["ts"] < start_date]

# ======================= CSS =======================
st.markdown("""
<style>
.block-container { padding-top: 2rem; padding-left: 2.5rem; padding-right: 2.5rem; }

.metric-card { padding: 20px 12px 16px; text-align: center; border-bottom: 2px solid #1e3556; }
.metric-value { font-size: 2.1rem; font-weight: 700; color: #5b9bd5; letter-spacing: -0.5px; }
.metric-label { font-size: .72rem; color: #4a6080; margin-top: 6px;
    letter-spacing: .1em; text-transform: uppercase; }

/* ── Panel box — removed ── */

.section-title {
    font-size: .68rem; font-weight: 700; letter-spacing: .14em;
    text-transform: uppercase; color: #3a5a8a;
    margin-bottom: 14px; margin-top: 0;
}

.ranking-row { display: flex; align-items: center; gap: 14px;
    padding: 9px 0; border-bottom: 1px solid #0e1d2c; }
.ranking-row:last-child { border-bottom: none; }

.ranking-img        { width:40px; height:40px; border-radius:50%; object-fit:cover; flex-shrink:0; opacity:.88; }
.ranking-img-square { width:40px; height:40px; border-radius:4px;  object-fit:cover; flex-shrink:0; opacity:.88; }
.ranking-placeholder    { width:40px; height:40px; border-radius:50%; background:#0f1e2e;
    display:flex; align-items:center; justify-content:center; font-size:1rem; flex-shrink:0; color:#2e4a6a; }
.ranking-placeholder-sq { width:40px; height:40px; border-radius:4px; background:#0f1e2e;
    display:flex; align-items:center; justify-content:center; font-size:1rem; flex-shrink:0; color:#2e4a6a; }

.ranking-pos { font-size:.72rem; font-weight:700; color:#1e3556;
    width:18px; text-align:right; flex-shrink:0; font-variant-numeric:tabular-nums; }
.ranking-pos.hi { color:#4a7ab5; }

.ranking-info { flex:1; min-width:0; }
.ranking-name { font-weight:600; color:#b8cce0; font-size:.86rem;
    white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.ranking-sub  { font-size:.72rem; color:#344d66; margin-top:2px;
    white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }

.ranking-val { font-size:.8rem; color:#4272a0; font-weight:600;
    flex-shrink:0; text-align:right; min-width:48px; font-variant-numeric:tabular-nums; }
</style>
""", unsafe_allow_html=True)

# ======================= HELPERS =======================
def fmt(hours):
    m = int(hours * 60)
    return f"{m//60}h {m%60}m"

def get_artist_img(artist):
    return st.session_state.enriched_cache["artists"].get(artist, {}).get("url_artist_img", "")

def get_album_img(artist, album):
    return st.session_state.enriched_cache["albums"].get(f"{artist}||{album}", {}).get("url_album_img", "")

def make_ranking(group_cols, src):
    return (src.groupby(group_cols)
            .agg(ascolti=("track_name","count"), ore=("hours","sum"))
            .sort_values("ore", ascending=False).reset_index())

def row_artist(i, row):
    img = get_artist_img(row["artist_name"])
    pc = "hi" if i < 3 else ""
    ih = f'<img class="ranking-img" src="{img}">' if img else '<div class="ranking-placeholder">♪</div>'
    return (f'<div class="ranking-row">'
            f'<div class="ranking-pos {pc}">{i+1:02d}</div>{ih}'
            f'<div class="ranking-info"><div class="ranking-name">{row["artist_name"]}</div></div>'
            f'<div class="ranking-val">{fmt(row["ore"])}</div></div>')

def row_artist_new(row):
    img = get_artist_img(row["artist_name"])
    ih = f'<img class="ranking-img" src="{img}">' if img else '<div class="ranking-placeholder">♪</div>'
    return (f'<div class="ranking-row">{ih}'
            f'<div class="ranking-info"><div class="ranking-name">{row["artist_name"]}</div></div>'
            f'<div class="ranking-val">{fmt(row["hours"])}</div></div>')

def row_track(i, row):
    img = get_album_img(row["artist_name"], row.get("album_name",""))
    pc = "hi" if i < 3 else ""
    ih = f'<img class="ranking-img-square" src="{img}">' if img else '<div class="ranking-placeholder-sq">♪</div>'
    return (f'<div class="ranking-row">'
            f'<div class="ranking-pos {pc}">{i+1:02d}</div>{ih}'
            f'<div class="ranking-info"><div class="ranking-name">{row["track_name"]}</div>'
            f'<div class="ranking-sub">{row["artist_name"]}</div></div>'
            f'<div class="ranking-val">{fmt(row["ore"])}</div></div>')

def row_track_new(row):
    img = get_album_img(row["artist_name"], "")
    ih = f'<img class="ranking-img-square" src="{img}">' if img else '<div class="ranking-placeholder-sq">♪</div>'
    return (f'<div class="ranking-row">{ih}'
            f'<div class="ranking-info"><div class="ranking-name">{row["track_name"]}</div>'
            f'<div class="ranking-sub">{row["artist_name"]}</div></div>'
            f'<div class="ranking-val">{fmt(row["hours"])}</div></div>')

def row_album(i, row):
    img = get_album_img(row["artist_name"], row["album_name"])
    pc = "hi" if i < 3 else ""
    ih = f'<img class="ranking-img-square" src="{img}">' if img else '<div class="ranking-placeholder-sq">◈</div>'
    return (f'<div class="ranking-row">'
            f'<div class="ranking-pos {pc}">{i+1:02d}</div>{ih}'
            f'<div class="ranking-info"><div class="ranking-name">{row["album_name"]}</div>'
            f'<div class="ranking-sub">{row["artist_name"]}</div></div>'
            f'<div class="ranking-val">{fmt(row["ore"])}</div></div>')

def row_album_new(row):
    img = get_album_img(row["artist_name"], row["album_name"])
    ih = f'<img class="ranking-img-square" src="{img}">' if img else '<div class="ranking-placeholder-sq">◈</div>'
    return (f'<div class="ranking-row">{ih}'
            f'<div class="ranking-info"><div class="ranking-name">{row["album_name"]}</div>'
            f'<div class="ranking-sub">{row["artist_name"]}</div></div>'
            f'<div class="ranking-val">{fmt(row["hours"])}</div></div>')

# ======================= RANKING =======================
artists_rank = make_ranking(["artist_name"], filtered_view)
tracks_rank  = make_ranking(["artist_name","track_name"], filtered_view)
albums_rank  = make_ranking(["artist_name","album_name"], filtered_view)

genre_rank = pd.DataFrame()
if "genre" in filtered_view.columns:
    gdf = filtered_view[filtered_view["genre"].notna() & (filtered_view["genre"] != "")]
    if not gdf.empty:
        genre_rank = (gdf.groupby("genre")
                      .agg(ascolti=("track_name","count"), ore=("hours","sum"))
                      .sort_values("ore", ascending=False).reset_index())

# ======================= METRICHE =======================
col1, col2, col3, col4 = st.columns(4)
for col, val, label in zip(
    [col1, col2, col3, col4],
    [f"{filtered_view['hours'].sum():.1f}",
     f"{filtered_view.groupby('data')['hours'].sum().mean():.1f}",
     str(filtered_view['track_name'].nunique()),
     str(filtered_view['artist_name'].nunique())],
    ["Totale ore ascoltate","Media ore giornaliere","Brani unici","Artisti unici"]
):
    with col:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{val}</div>'
                    f'<div class="metric-label">{label}</div></div>', unsafe_allow_html=True)

# ======================= TOP 10 + NOVITÀ =======================
st.write("")
st.write("")

tab_artists, tab_tracks, tab_albums, tab_genres = st.tabs(
    ["Top Artisti","Top Brani","Top Album","Top Generi"])

# ── Artisti ──
with tab_artists:
    top10 = artists_rank.head(10).reset_index(drop=True)
    new_a = (filtered_view[~filtered_view["artist_name"].isin(history_df["artist_name"])]
             .groupby("artist_name")["hours"].sum()
             .pipe(lambda s: s[s >= 1]).sort_values(ascending=False).head(10).reset_index())

    col_l, col_r = st.columns([2, 1])
    with col_l:
        html = '<div class="section-title">Top 10 Artisti</div>'
        for i, row in top10.iterrows():
            html += row_artist(i, row)
        st.markdown(html, unsafe_allow_html=True)
    with col_r:
        if not new_a.empty:
            html = '<div class="section-title">Top Novità</div>'
            for _, row in new_a.iterrows():
                html += row_artist_new(row)
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.info("Nessuna novità.")

# ── Brani ──
with tab_tracks:
    top10 = tracks_rank.head(10).reset_index(drop=True)
    hist_keys = set(history_df["artist_name"] + "||" + history_df["track_name"])
    new_t = (filtered_view[~(filtered_view["artist_name"] + "||" + filtered_view["track_name"]).isin(hist_keys)]
             .groupby(["artist_name","track_name"])["hours"].sum().reset_index()
             .pipe(lambda d: d[d["hours"] >= 0.5]).sort_values("hours", ascending=False).head(10))

    col_l, col_r = st.columns([2, 1])
    with col_l:
        html = '<div class="section-title">Top 10 Brani</div>'
        for i, row in top10.iterrows():
            html += row_track(i, row)
        st.markdown(html, unsafe_allow_html=True)
    with col_r:
        if not new_t.empty:
            html = '<div class="section-title">Top Novità</div>'
            for _, row in new_t.iterrows():
                html += row_track_new(row)
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.info("Nessuna novità.")

# ── Album ──
with tab_albums:
    top10 = albums_rank.head(10).reset_index(drop=True)
    hist_keys = set(history_df["artist_name"] + "||" + history_df["album_name"])
    new_al = (filtered_view[~(filtered_view["artist_name"] + "||" + filtered_view["album_name"]).isin(hist_keys)]
              .groupby(["artist_name","album_name"])["hours"].sum().reset_index()
              .pipe(lambda d: d[d["hours"] >= 0.5]).sort_values("hours", ascending=False).head(10))

    col_l, col_r = st.columns([2, 1])
    with col_l:
        html = '<div class="section-title">Top 10 Album</div>'
        for i, row in top10.iterrows():
            html += row_album(i, row)
        st.markdown(html, unsafe_allow_html=True)
    with col_r:
        if not new_al.empty:
            html = '<div class="section-title">Top Novità</div>'
            for _, row in new_al.iterrows():
                html += row_album_new(row)
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.info("Nessuna novità.")

# ── Generi ──
with tab_genres:
    if genre_rank.empty:
        st.info("Nessun genere disponibile.")
    else:
        top10 = genre_rank.head(10).reset_index(drop=True)

        if history_df.empty:
            hist_genres = set()
        elif "genre" in history_df.columns:
            hist_genres = set(history_df["genre"].dropna().unique())
        else:
            agm = pd.DataFrame([{"artist_name": k, "genre": v.get("genre","")}
                                 for k, v in st.session_state.enriched_cache["artists"].items()
                                 if v.get("genre","")])
            hist_genres = set(
                history_df[["artist_name"]].drop_duplicates()
                .merge(agm, on="artist_name", how="left")["genre"].dropna().unique()
            ) if not agm.empty else set()

        new_g = genre_rank[~genre_rank["genre"].isin(hist_genres)].head(10)

        genre_emojis = {"pop":"🎤","rock":"🎸","hip hop":"🎤","rap":"🎤","electronic":"🎛️",
                        "jazz":"🎷","classical":"🎻","r&b":"🎙️","indie":"🎵","metal":"🤘",
                        "country":"🤠","latin":"💃","soul":"🎶","folk":"🪕","reggae":"🌿","blues":"🎺"}
        def gem(g):
            g = g.lower()
            for k, v in genre_emojis.items():
                if k in g: return v
            return "🎼"

        def row_genre(i, row):
            pc = "hi" if i < 3 else ""
            return (f'<div class="ranking-row">'
                    f'<div class="ranking-pos {pc}">{i+1:02d}</div>'
                    f'<div class="ranking-placeholder">{gem(row["genre"])}</div>'
                    f'<div class="ranking-info"><div class="ranking-name">{row["genre"].title()}</div></div>'
                    f'<div class="ranking-val">{fmt(row["ore"])}</div></div>')

        def row_genre_new(row):
            return (f'<div class="ranking-row">'
                    f'<div class="ranking-placeholder">{gem(row["genre"])}</div>'
                    f'<div class="ranking-info"><div class="ranking-name">{row["genre"].title()}</div></div>'
                    f'<div class="ranking-val">{fmt(row["ore"])}</div></div>')

        col_l, col_r = st.columns([2, 1])
        with col_l:
            html = '<div class="section-title">Top 10 Generi</div>'
            for i, row in top10.iterrows():
                html += row_genre(i, row)
            st.markdown(html, unsafe_allow_html=True)
        with col_r:
            if not new_g.empty:
                html = '<div class="section-title">Top Novità</div>'
                for _, row in new_g.iterrows():
                    html += row_genre_new(row)
                st.markdown(html, unsafe_allow_html=True)
            else:
                st.info("Nessuna novità.")

# ======================= GRAFICO ORE =======================
st.divider()
st.header("Ore totali ascoltate")
col_radio, _ = st.columns([1, 2])
with col_radio:
    agg_window = st.radio("Raggruppa per", ["Giorno","Settimana","Mese"], horizontal=True)

freq_map = {"Giorno": "D", "Settimana": "W-MON", "Mese": "ME"}
freq = freq_map[agg_window]

if not filtered_view.empty:
    if agg_window == "Giorno":
        plot_data = filtered_view.groupby("data")["hours"].sum().reset_index()
        plot_data = plot_data.rename(columns={"data": "ts"})
        plot_data["ts"] = pd.to_datetime(plot_data["ts"])
    else:
        plot_data = filtered_view.groupby(pd.Grouper(key="ts", freq=freq))["hours"].sum().reset_index()

    title = f"Ore per {agg_window.lower()}" + (f" — {selected_artist}" if selected_artist != "Tutti" else "")
    fig = px.bar(plot_data, x="ts", y="hours",
                 labels={"ts":"Data","hours":"Ore"}, title=title,
                 color_discrete_sequence=["#2a5a9a"])
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                      font_color="#6a8aaa", title_font_color="#4a7ab5")
    fig.update_xaxes(range=[start_date, end_date], gridcolor="#0f1e2e")
    fig.update_yaxes(gridcolor="#0f1e2e")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Nessun dato per i filtri selezionati.")

# ======================= DISTRIBUZIONE PER DECENNIO =======================
st.divider()
st.header("📅 Ascolti per decennio di pubblicazione")

if "year_published" in filtered_view.columns:
    plot_df = filtered_view[filtered_view["year_published"].notna() & (filtered_view["year_published"] != "")].copy()
    if not plot_df.empty:
        plot_df["release_year"] = pd.to_numeric(plot_df["year_published"], errors="coerce")
        plot_df = plot_df.dropna(subset=["release_year"])
        plot_df["decade"] = (plot_df["release_year"] // 10) * 10
        decade_group = plot_df.groupby("decade")["hours"].sum().reset_index().sort_values("decade")
        fig_d = px.bar(decade_group, x="decade", y="hours",
                       labels={"decade":"Decennio","hours":"Ore ascoltate"},
                       title="Distribuzione delle ore ascoltate per decennio di pubblicazione",
                       color_discrete_sequence=["#2a5a9a"])
        fig_d.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                            font_color="#6a8aaa", title_font_color="#4a7ab5")
        fig_d.update_xaxes(tickformat="d", gridcolor="#0f1e2e")
        fig_d.update_yaxes(gridcolor="#0f1e2e")
        st.plotly_chart(fig_d, use_container_width=True)
    else:
        st.info("Nessun dato con anno di uscita valido.")
else:
    st.info("Anno di pubblicazione non disponibile.")

# ======================= DATAFRAME COMPLETO =======================
st.divider()
st.header("🗃️ DataFrame completo")
st.dataframe(filtered_view.drop(columns=["ts","hours","year"], errors="ignore"), use_container_width=True)