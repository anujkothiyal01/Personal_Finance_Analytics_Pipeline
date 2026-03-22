import streamlit as st
import pandas as pd
from snowflake.snowpark import Session

st.set_page_config(
    page_title="Personal Finance Analytics Dashboard",
    page_icon="💸",
    layout="wide"
)

# -----------------------------
# Helpers
# -----------------------------
def safe_float(value, default=0.0):
    if pd.isna(value) or value is None:
        return default
    return float(value)


def format_inr(value):
    return f"₹{safe_float(value):,.2f}"


# -----------------------------
# Snowflake connection
# -----------------------------
def get_snowflake_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        connection_parameters = {
            "account": "CYFIKTH-XB07314",
            "user": "AK",
            "password": "Anuj@123456789",
            "role": "ACCOUNTADMIN",
            "warehouse": "COMPUTE_WH",
            "database": "PERSONAL_FINANCE_ANALYTICS_PIPELINE",
            "schema": "GOLD",
        }
        return Session.builder.configs(connection_parameters).create()


session = get_snowflake_session()


# -----------------------------
# Data loaders
# -----------------------------
@st.cache_data(ttl=300)
def load_kpis() -> pd.DataFrame:
    query = """
        SELECT *
        FROM PERSONAL_FINANCE_ANALYTICS_PIPELINE.GOLD.MART_SAVINGS_KPIS
    """
    return session.sql(query).to_pandas()


@st.cache_data(ttl=300)
def load_monthly_cashflow() -> pd.DataFrame:
    query = """
        SELECT *
        FROM PERSONAL_FINANCE_ANALYTICS_PIPELINE.GOLD.MART_MONTHLY_CASHFLOW
        ORDER BY MONTH_START_DATE
    """
    df = session.sql(query).to_pandas()
    if not df.empty:
        df["MONTH_START_DATE"] = pd.to_datetime(df["MONTH_START_DATE"])
        df["MONTH_LABEL"] = df["MONTH_START_DATE"].dt.strftime("%Y-%m")
    return df


@st.cache_data(ttl=300)
def load_category_spend() -> pd.DataFrame:
    query = """
        SELECT *
        FROM PERSONAL_FINANCE_ANALYTICS_PIPELINE.GOLD.MART_CATEGORY_SPEND
        ORDER BY MONTH_START_DATE, CATEGORY, SUBCATEGORY
    """
    df = session.sql(query).to_pandas()
    if not df.empty:
        df["MONTH_START_DATE"] = pd.to_datetime(df["MONTH_START_DATE"])
        df["MONTH_LABEL"] = df["MONTH_START_DATE"].dt.strftime("%Y-%m")
    return df


kpi_df = load_kpis()
cashflow_df = load_monthly_cashflow()
category_df = load_category_spend()

# -----------------------------
# Header
# -----------------------------
st.title("💸 Personal Finance Analytics Dashboard")
st.caption("Built on Snowflake + dbt + Streamlit")

# -----------------------------
# KPI cards
# -----------------------------
if not kpi_df.empty:
    row = kpi_df.iloc[0]

    total_income = safe_float(row.get("TOTAL_INCOME"))
    total_expense = safe_float(row.get("TOTAL_EXPENSE"))
    total_transfer = safe_float(row.get("TOTAL_TRANSFER"))
    recurring_expense_total = safe_float(row.get("RECURRING_EXPENSE_TOTAL"))
    net_savings = safe_float(row.get("NET_SAVINGS"))
    savings_rate = safe_float(row.get("SAVINGS_RATE")) * 100

    c1, c2, c3 = st.columns(3)
    c4, c5, c6 = st.columns(3)

    c1.metric("Total Income", format_inr(total_income))
    c2.metric("Total Expense", format_inr(total_expense))
    c3.metric("Total Transfer", format_inr(total_transfer))
    c4.metric("Recurring Expense", format_inr(recurring_expense_total))
    c5.metric("Net Savings", format_inr(net_savings))
    c6.metric("Savings Rate", f"{savings_rate:.2f}%")
else:
    st.warning("No KPI data found.")

st.divider()

# -----------------------------
# Filters
# -----------------------------
st.subheader("Filters")

available_months = []
if not cashflow_df.empty:
    available_months = cashflow_df["MONTH_LABEL"].dropna().unique().tolist()

selected_months = st.multiselect(
    "Select month(s)",
    options=available_months,
    default=available_months
)

available_categories = []
if not category_df.empty:
    temp_categories = category_df.copy()
    temp_categories = temp_categories[
        ~temp_categories["CATEGORY"].isin(["income", "transfer"])
    ]
    available_categories = sorted(temp_categories["CATEGORY"].dropna().unique().tolist())

selected_categories = st.multiselect(
    "Select category(s)",
    options=available_categories,
    default=available_categories
)

filtered_cashflow_df = cashflow_df.copy()
if selected_months:
    filtered_cashflow_df = filtered_cashflow_df[
        filtered_cashflow_df["MONTH_LABEL"].isin(selected_months)
    ]

filtered_category_df = category_df.copy()
if selected_months:
    filtered_category_df = filtered_category_df[
        filtered_category_df["MONTH_LABEL"].isin(selected_months)
    ]

# Only expense categories for spend visuals
filtered_category_df = filtered_category_df[
    ~filtered_category_df["CATEGORY"].isin(["income", "transfer"])
]

if selected_categories:
    filtered_category_df = filtered_category_df[
        filtered_category_df["CATEGORY"].isin(selected_categories)
    ]

st.divider()

# -----------------------------
# Monthly cashflow trend
# -----------------------------
st.subheader("Monthly Cashflow Trend")

if not filtered_cashflow_df.empty:
    chart_df = filtered_cashflow_df.copy()
    chart_df = chart_df[[
        "MONTH_LABEL",
        "TOTAL_INCOME",
        "TOTAL_EXPENSE",
        "NET_SAVINGS"
    ]].sort_values("MONTH_LABEL")

    chart_df = chart_df.set_index("MONTH_LABEL")
    st.line_chart(chart_df)

    display_cashflow_df = filtered_cashflow_df[[
        "MONTH_LABEL",
        "TOTAL_INCOME",
        "TOTAL_EXPENSE",
        "TOTAL_TRANSFER",
        "NET_SAVINGS",
        "SAVINGS_RATE"
    ]].copy()

    display_cashflow_df["SAVINGS_RATE"] = (
        display_cashflow_df["SAVINGS_RATE"].fillna(0) * 100
    ).round(2)

    st.dataframe(display_cashflow_df, use_container_width=True)
else:
    st.info("No monthly cashflow data available for selected filters.")

st.divider()

# -----------------------------
# Category spend breakdown
# -----------------------------
st.subheader("Category Spend Breakdown")

if not filtered_category_df.empty:
    category_summary = (
        filtered_category_df.groupby("CATEGORY", as_index=False)["TOTAL_EXPENSE"]
        .sum()
        .sort_values("TOTAL_EXPENSE", ascending=False)
    )

    st.bar_chart(category_summary.set_index("CATEGORY")["TOTAL_EXPENSE"])

    display_category_df = filtered_category_df[[
        "MONTH_LABEL",
        "CATEGORY",
        "SUBCATEGORY",
        "TOTAL_EXPENSE",
        "TRANSACTION_COUNT"
    ]].copy()

    st.dataframe(display_category_df, use_container_width=True)
else:
    st.info("No category spend data available for selected filters.")

st.divider()

# -----------------------------
# Top spending categories
# -----------------------------
st.subheader("Top Spending Categories")

if not filtered_category_df.empty:
    top_categories = (
        filtered_category_df.groupby(
            ["CATEGORY", "SUBCATEGORY"], as_index=False
        )["TOTAL_EXPENSE"]
        .sum()
        .sort_values("TOTAL_EXPENSE", ascending=False)
        .head(10)
    )

    st.dataframe(top_categories, use_container_width=True)
else:
    st.info("No top category data available.")

st.divider()

# -----------------------------
# Optional debug section
# -----------------------------
with st.expander("Debug data preview"):
    st.write("KPI shape:", kpi_df.shape)
    st.write("Cashflow shape:", cashflow_df.shape)
    st.write("Category shape:", category_df.shape)

    st.write("KPI columns:", kpi_df.columns.tolist())
    st.write("Cashflow columns:", cashflow_df.columns.tolist())
    st.write("Category columns:", category_df.columns.tolist())

st.caption("Source: Snowflake gold marts generated via dbt models.")