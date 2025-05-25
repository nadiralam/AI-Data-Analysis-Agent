import json
import tempfile
import csv
import streamlit as st
import pandas as pd
import re
import duckdb
import google.generativeai as genai

# Custom Gemini wrapper for SQL-like interaction
class GeminiChatWrapper:
    def __init__(self, api_key, model="gemini-1.5-flash"):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model)
    
    def run(self, prompt):
        response = self.model.generate_content(prompt)
        return response.text

# Function to preprocess and save the uploaded file
def preprocess_and_save(file):
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file, encoding='utf-8', na_values=['NA', 'N/A', 'missing'])
        elif file.name.endswith('.xlsx'):
            df = pd.read_excel(file, na_values=['NA', 'N/A', 'missing'])
        else:
            st.error("Unsupported file format. Please upload a CSV or Excel file.")
            return None, None, None
        
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].astype(str).replace({r'"': '""'}, regex=True)
        
        for col in df.columns:
            if 'date' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    pass
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_path = temp_file.name
            df.to_csv(temp_path, index=False, quoting=csv.QUOTE_ALL)
        
        return temp_path, df.columns.tolist(), df
    except Exception as e:
        st.error(f"Error processing file: {e}")
        return None, None, None

# Streamlit App UI
st.title("📊 Gemini-Powered Data Analyst")

with st.sidebar:
    st.header("Gemini API Key")
    gemini_key = st.text_input("Enter your Gemini API key:", type="password")
    if gemini_key:
        st.session_state.gemini_key = gemini_key
        st.success("API key saved!")
    else:
        st.warning("Please enter your Gemini API key to proceed.")

uploaded_file = st.file_uploader("Upload a CSV or Excel file", type=["csv", "xlsx"])

if uploaded_file is not None and "gemini_key" in st.session_state:
    temp_path, columns, df = preprocess_and_save(uploaded_file)
    
    if temp_path and columns and df is not None:
        st.write("Uploaded Data:")
        st.dataframe(df)
        st.write("Columns:", columns)

        con = duckdb.connect()
        con.execute(f"CREATE TABLE uploaded_data AS SELECT * FROM read_csv_auto('{temp_path}')")

        gemini_agent = GeminiChatWrapper(api_key=st.session_state.gemini_key)

        user_query = st.text_area("Ask a question about your data:")
        st.info("💡 Example: What is the average sales by region?")

        if st.button("Submit Query"):
            if not user_query.strip():
                st.warning("Please enter a valid query.")
            else:
                with st.spinner("Processing with Gemini..."):
                    prompt = (
                        f"You are a data analyst. Given the table `uploaded_data`, "
                        f"generate a SQL query in DuckDB dialect to answer the following:\n\n"
                        f"{user_query}\n\n"
                        f"Return only the SQL query inside triple backticks (```sql)."
                    )
                    try:
                        gemini_response = gemini_agent.run(prompt)
                        sql_match = re.search(r"```sql\n(.*?)```", gemini_response, re.DOTALL)

                        if sql_match:
                            sql_query = sql_match.group(1).strip()
                            st.code(sql_query, language="sql")
                            result = con.execute(sql_query).df()
                            st.success("Query executed successfully!")
                            st.dataframe(result)
                        else:
                            st.warning("Gemini did not return a valid SQL query. Try rephrasing.")
                            st.markdown(gemini_response)
                    except Exception as e:
                        st.error(f"Failed to run Gemini query: {e}")
