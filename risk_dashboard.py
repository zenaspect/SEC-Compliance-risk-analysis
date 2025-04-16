import pandas as pd
import streamlit as st
import plotly.express as px

# Load data
df = pd.read_csv("C:/codes/Data_Analytics/risk_scores2.csv")

st.set_page_config(page_title="SEC Risk Dashboard", layout="wide")

st.title("📊 SEC Filing Risk Score Dashboard")

# Filters
tickers = st.multiselect("Select Ticker(s):", sorted(df['ticker'].unique()), default=None)
forms = st.multiselect("Select Form Type(s):", sorted(df['form'].unique()), default=None)
date_range = st.date_input("Select Date Range:", [])

# Filter data
if tickers:
    df = df[df['ticker'].isin(tickers)]
if forms:
    df = df[df['form'].isin(forms)]
if len(date_range) == 2:
    df['filing_date'] = pd.to_datetime(df['filing_date'], errors='coerce')
    df = df[(df['filing_date'] >= pd.to_datetime(date_range[0])) & (df['filing_date'] <= pd.to_datetime(date_range[1]))]

st.markdown(f"### Showing {len(df)} records")

# Risk category count
st.subheader("Risk Category Distribution")
fig_cat = px.histogram(df, x="risk_category", color="risk_category", title="Risk Categories", nbins=6)
st.plotly_chart(fig_cat, use_container_width=True)

# Risk score trend
st.subheader("Risk Score Trend Over Time")
fig_trend = px.line(df.sort_values("filing_date"), x="filing_date", y="risk_score", color="ticker", markers=True)
st.plotly_chart(fig_trend, use_container_width=True)

# Top risky filings
st.subheader("Top 5 Riskiest Filings")
top5 = df.sort_values(by="risk_score", ascending=False).head(5)
st.dataframe(top5[['ticker', 'form', 'filing_date', 'risk_score', 'risk_category']])

# Full table
st.markdown("### 📄 Full Data Table")
st.dataframe(df)

