import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client, Client
from postgrest.exceptions import APIError

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Dashboard Performance Retail",
    page_icon="📊",
    layout="wide"
)

# =========================================================
# HEADER
# =========================================================
st.title("📊 Dashboard Performance Retail")
st.markdown(
    """
    Suivi de la performance des magasins à partir des données **Supabase**.  
    Cette application permet d’analyser le **chiffre d’affaires**, le **nombre de tickets**,
    la **quantité vendue** et la **performance des magasins**.
    """
)
st.markdown("---")

# =========================================================
# CONFIGURATION
# =========================================================
TABLES = ["data_bi_franchise", "data_bi_succursale"]

COLUMN_MAPPING = {
    "date": "Ticket Date",
    "store_id": "Code Magasin",
    "store_name": "Nom magasin",
    "quantity": "Quantite",
    "orders": "Numero Ticket",
    "revenue": "Total TTC Net",
    "revenue_ht": "Total HT Net",
}

# =========================================================
# SUPABASE CONNECTION
# =========================================================
@st.cache_resource
def get_supabase_client() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

client = get_supabase_client()

# =========================================================
# UTILS
# =========================================================
def clean_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace("\u202f", "", regex=False)
        .str.replace("\xa0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^0-9.\-]", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)

def format_number(value):
    try:
        return f"{value:,.0f}".replace(",", " ")
    except Exception:
        return value

def format_currency(value):
    try:
        return f"{value:,.2f} €".replace(",", " ").replace(".", ",")
    except Exception:
        return value

def format_percent(value):
    try:
        return f"{value:,.2f} %".replace(",", " ").replace(".", ",")
    except Exception:
        return value

def fetch_all_rows(table_name: str, page_size: int = 1000):
    all_rows = []
    start = 0

    while True:
        end = start + page_size - 1
        response = client.table(table_name).select("*").range(start, end).execute()
        batch = response.data if response.data else []

        if not batch:
            break

        all_rows.extend(batch)

        if len(batch) < page_size:
            break

        start += page_size

    return all_rows

def fetch_table_safe(table_name: str) -> pd.DataFrame:
    try:
        rows = fetch_all_rows(table_name)
        data = pd.DataFrame(rows)

        if not data.empty:
            if table_name == "data_bi_franchise":
                data["source_table"] = "Franchise"
            elif table_name == "data_bi_succursale":
                data["source_table"] = "Succursale"
            else:
                data["source_table"] = table_name

        return data

    except APIError as e:
        st.warning(f"Impossible de charger la table '{table_name}' : {e}")
        return pd.DataFrame()

# =========================================================
# LOAD DATA
# =========================================================
@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    frames = []

    for table_name in TABLES:
        df = fetch_table_safe(table_name)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True)

    required_cols = [
        COLUMN_MAPPING["date"],
        COLUMN_MAPPING["store_name"],
        COLUMN_MAPPING["revenue"],
    ]

    missing_cols = [col for col in required_cols if col not in data.columns]
    if missing_cols:
        st.error("Colonnes obligatoires manquantes : " + ", ".join(missing_cols))
        st.write("Colonnes disponibles :", list(data.columns))
        return pd.DataFrame()

    data[COLUMN_MAPPING["date"]] = pd.to_datetime(
        data[COLUMN_MAPPING["date"]],
        errors="coerce"
    )

    numeric_cols = [
        COLUMN_MAPPING["quantity"],
        COLUMN_MAPPING["orders"],
        COLUMN_MAPPING["revenue"],
        COLUMN_MAPPING["revenue_ht"],
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = clean_numeric(data[col])

    for col in [COLUMN_MAPPING["store_name"]]:
        if col in data.columns:
            data[col] = data[col].astype(str).str.strip()

    return data

data = load_data()

if data.empty:
    st.warning(
        "Aucune donnée disponible. Vérifie les noms des tables Supabase ou les colonnes importées."
    )
    st.stop()

# =========================================================
# COLUMN SHORTCUTS
# =========================================================
date_col = COLUMN_MAPPING["date"]
store_id_col = COLUMN_MAPPING["store_id"]
store_name_col = COLUMN_MAPPING["store_name"]
quantity_col = COLUMN_MAPPING["quantity"]
orders_col = COLUMN_MAPPING["orders"]
revenue_col = COLUMN_MAPPING["revenue"]
revenue_ht_col = COLUMN_MAPPING["revenue_ht"]

# =========================================================
# SIDEBAR FILTERS
# =========================================================
st.sidebar.header("🎛️ Filtres")

filtered_data = data.copy()

# Filtre période
min_date = filtered_data[date_col].min()
max_date = filtered_data[date_col].max()

if pd.notnull(min_date) and pd.notnull(max_date):
    selected_dates = st.sidebar.date_input(
        "Période",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date = end_date = selected_dates

    filtered_data = filtered_data[
        (filtered_data[date_col].dt.date >= start_date)
        & (filtered_data[date_col].dt.date <= end_date)
    ]

# Filtre source
if "source_table" in filtered_data.columns:
    source_options = sorted(
        filtered_data["source_table"].dropna().astype(str).unique().tolist()
    )
    selected_sources = st.sidebar.multiselect(
        "Source",
        source_options,
        default=source_options
    )
    if selected_sources:
        filtered_data = filtered_data[
            filtered_data["source_table"].astype(str).isin(selected_sources)
        ]

# Filtre magasin : plus propre qu'un multiselect géant
if store_name_col in filtered_data.columns:
    store_options = sorted(
        filtered_data[store_name_col].dropna().astype(str).unique().tolist()
    )
    selected_store = st.sidebar.selectbox(
        "Magasin",
        ["Tous"] + store_options
    )

    if selected_store != "Tous":
        filtered_data = filtered_data[
            filtered_data[store_name_col].astype(str) == selected_store
        ]

if filtered_data.empty:
    st.warning("Aucune donnée disponible avec les filtres sélectionnés.")
    st.stop()

# =========================================================
# KPI
# =========================================================
def safe_sum(df, col):
    return df[col].sum() if col in df.columns else 0

current_revenue = safe_sum(filtered_data, revenue_col)
current_revenue_ht = safe_sum(filtered_data, revenue_ht_col)
current_orders = safe_sum(filtered_data, orders_col)
current_quantity = safe_sum(filtered_data, quantity_col)

avg_basket = current_revenue / current_orders if current_orders else 0
avg_qty_per_ticket = current_quantity / current_orders if current_orders else 0

st.markdown("## 📊 Indicateurs clés")

k1, k2, k3 = st.columns(3)
k4, k5, k6 = st.columns(3)

k1.metric("CA TTC Net", format_currency(current_revenue))
k2.metric("CA HT Net", format_currency(current_revenue_ht))
k3.metric("Nb tickets", format_number(current_orders))
k4.metric("Quantité vendue", format_number(current_quantity))
k5.metric("Panier moyen", format_currency(avg_basket))
k6.metric(
    "Qté moyenne / ticket",
    f"{avg_qty_per_ticket:,.2f}".replace(",", " ").replace(".", ",")
)

# =========================================================
# PERFORMANCE PAR MAGASIN
# =========================================================
group_cols = [store_name_col]
if store_id_col in filtered_data.columns:
    group_cols.append(store_id_col)
if "source_table" in filtered_data.columns:
    group_cols.append("source_table")

agg_dict = {revenue_col: "sum"}
if revenue_ht_col in filtered_data.columns:
    agg_dict[revenue_ht_col] = "sum"
if orders_col in filtered_data.columns:
    agg_dict[orders_col] = "sum"
if quantity_col in filtered_data.columns:
    agg_dict[quantity_col] = "sum"

store_perf = filtered_data.groupby(group_cols, as_index=False).agg(agg_dict)

if orders_col in store_perf.columns:
    store_perf["panier_moyen"] = (
        store_perf[revenue_col] / store_perf[orders_col].replace(0, pd.NA)
    )
    if quantity_col in store_perf.columns:
        store_perf["qte_moyenne_ticket"] = (
            store_perf[quantity_col] / store_perf[orders_col].replace(0, pd.NA)
        )
    else:
        store_perf["qte_moyenne_ticket"] = pd.NA
else:
    store_perf["panier_moyen"] = pd.NA
    store_perf["qte_moyenne_ticket"] = pd.NA

total_revenue = store_perf[revenue_col].sum() if revenue_col in store_perf.columns else 0
if total_revenue:
    store_perf["part_ca_pct"] = (store_perf[revenue_col] / total_revenue) * 100
else:
    store_perf["part_ca_pct"] = 0

# =========================================================
# INSIGHTS
# =========================================================
st.markdown("## 🧠 Insights clés")

best_store = store_perf.sort_values(revenue_col, ascending=False).iloc[0]
worst_store = store_perf.sort_values(revenue_col, ascending=True).iloc[0]
nb_active_stores = store_perf[store_name_col].nunique()

i1, i2, i3 = st.columns(3)
i1.info(f"🏆 Meilleur magasin : {best_store[store_name_col]}")
i2.warning(f"📉 Moins performant : {worst_store[store_name_col]}")
i3.success(f"🏬 Magasins actifs : {format_number(nb_active_stores)}")

st.divider()

# =========================================================
# EVOLUTION TEMPORELLE
# =========================================================
st.subheader("📈 Évolution du chiffre d’affaires")

if filtered_data[date_col].nunique() < 2:
    st.info("Pas assez de données pour afficher une évolution temporelle sur la période sélectionnée.")
else:
    time_agg = {revenue_col: "sum"}
    if revenue_ht_col in filtered_data.columns:
        time_agg[revenue_ht_col] = "sum"

    time_data = (
        filtered_data.groupby(date_col, as_index=False)
        .agg(time_agg)
        .sort_values(date_col)
    )

    y_cols = [revenue_col]
    if revenue_ht_col in time_data.columns:
        y_cols.append(revenue_ht_col)

    fig_time = px.line(
        time_data,
        x=date_col,
        y=y_cols,
        markers=True,
        title="Évolution du CA dans le temps"
    )
    fig_time.update_layout(
        xaxis_title="Date",
        yaxis_title="Montant",
        legend_title="Indicateurs"
    )
    st.plotly_chart(fig_time, use_container_width=True)

st.divider()

# =========================================================
# REPARTITIONS
# =========================================================
left, right = st.columns(2)

with left:
    st.subheader("🥧 Répartition du CA par source")
    if "source_table" in filtered_data.columns:
        source_perf = filtered_data.groupby("source_table", as_index=False)[revenue_col].sum()
        fig_source = px.pie(
            source_perf,
            names="source_table",
            values=revenue_col,
            title="Poids des sources dans le CA"
        )
        st.plotly_chart(fig_source, use_container_width=True)
    else:
        st.info("Aucune colonne source_table disponible.")

with right:
    st.subheader("🏬 Répartition du CA par magasin")
    store_split = (
        store_perf[[store_name_col, revenue_col]]
        .sort_values(revenue_col, ascending=False)
        .head(10)
    )
    fig_store_split = px.bar(
        store_split,
        x=store_name_col,
        y=revenue_col,
        title="Top 10 magasins par CA"
    )
    fig_store_split.update_layout(
        xaxis_title="Magasin",
        yaxis_title="CA TTC Net"
    )
    st.plotly_chart(fig_store_split, use_container_width=True)

st.divider()

# =========================================================
# PERFORMANCE MAGASINS
# =========================================================
st.subheader("🏪 Performance magasins")

top_col, flop_col = st.columns(2)

with top_col:
    st.markdown("**Top 10 magasins par CA TTC Net**")
    top10 = store_perf.sort_values(revenue_col, ascending=False).head(10).copy()
    st.dataframe(top10, use_container_width=True)

with flop_col:
    st.markdown("**Flop 10 magasins par CA TTC Net**")
    flop10 = store_perf.sort_values(revenue_col, ascending=True).head(10).copy()
    st.dataframe(flop10, use_container_width=True)

st.divider()

# =========================================================
# CHARTS PAR MAGASIN
# =========================================================
c1, c2 = st.columns(2)

with c1:
    st.subheader("🏆 Top 15 magasins par CA")
    top_chart = store_perf.sort_values(revenue_col, ascending=False).head(15)
    fig_store = px.bar(
        top_chart,
        x=store_name_col,
        y=revenue_col,
        title="Top 15 magasins"
    )
    fig_store.update_layout(
        xaxis_title="Magasin",
        yaxis_title="CA TTC Net"
    )
    st.plotly_chart(fig_store, use_container_width=True)

with c2:
    st.subheader("📊 Contribution au CA")
    part_chart = store_perf.sort_values("part_ca_pct", ascending=False).head(15)
    fig_part = px.bar(
        part_chart,
        x=store_name_col,
        y="part_ca_pct",
        title="Part du CA par magasin"
    )
    fig_part.update_layout(
        xaxis_title="Magasin",
        yaxis_title="% du CA"
    )
    st.plotly_chart(fig_part, use_container_width=True)

st.divider()

# =========================================================
# TABLE DETAILLEE
# =========================================================
st.subheader("📋 Table de performance détaillée")

display_cols = [col for col in [
    store_id_col,
    store_name_col,
    "source_table",
    revenue_col,
    revenue_ht_col,
    orders_col,
    quantity_col,
    "panier_moyen",
    "qte_moyenne_ticket",
    "part_ca_pct",
] if col in store_perf.columns]

table_display = store_perf[display_cols].sort_values(revenue_col, ascending=False).copy()

for col in [revenue_col, revenue_ht_col, "panier_moyen", "qte_moyenne_ticket", "part_ca_pct"]:
    if col in table_display.columns:
        table_display[col] = table_display[col].round(2)

st.dataframe(table_display, use_container_width=True)

# =========================================================
# EXPORT
# =========================================================
csv = table_display.to_csv(index=False).encode("utf-8")

st.download_button(
    label="📥 Télécharger la table de performance",
    data=csv,
    file_name="performance_magasins.csv",
    mime="text/csv",
)

# =========================================================
# DEBUG
# =========================================================
with st.expander("🔎 Debug / Aperçu des données"):
    st.write("Tables chargées :", TABLES)
    st.write("Colonnes disponibles :", list(data.columns))
    st.write("Nombre de lignes chargées :", len(data))

    if revenue_col in data.columns:
        st.write("Exemple valeurs CA TTC Net :", data[revenue_col].head(10))

    if quantity_col in data.columns:
        st.write("Exemple valeurs Quantité :", data[quantity_col].head(10))

    if revenue_ht_col in data.columns:
        st.write("Exemple valeurs CA HT Net :", data[revenue_ht_col].head(10))

    st.dataframe(data.head(10))