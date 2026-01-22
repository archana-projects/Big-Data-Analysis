import streamlit as st
from big_data import run_analysis

st.set_page_config(page_title="Big Data Analysis", layout="centered")

st.title("📊 Big Data Analysis Dashboard")
st.write("Using PySpark for scalable data processing")

if st.button("Run Analysis"):
    with st.spinner("Processing big data..."):
        category_sales, avg_order, region_orders = run_analysis()

    st.subheader("Category-wise Sales")
    st.dataframe(category_sales)

    st.subheader("Average Order Value")
    st.dataframe(avg_order)

    st.subheader("Region-wise Orders")
    st.dataframe(region_orders)

    st.success("Analysis Completed Successfully!")

