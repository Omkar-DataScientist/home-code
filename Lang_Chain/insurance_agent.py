import streamlit as st
import requests
import pdfplumber
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import ollama
import tempfile

st.set_page_config(page_title="🛡️ Insurance Policy Agent")

st.title("🤖 Insurance Policy Assistant (Gemma 3 - Local)")

# --- Phase 1: File or Link Upload ---
source_type = st.radio("Select input method:", ["📄 Upload PDF", "🔗 Enter URL"])

policy_text = ""

if source_type == "📄 Upload PDF":
    uploaded_file = st.file_uploader("Upload Insurance PDF", type=["pdf"])
    if uploaded_file:
        with pdfplumber.open(uploaded_file) as pdf:
            policy_text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
elif source_type == "🔗 Enter URL":
    url = st.text_input("Enter a link to the policy document (PDF or text):")
    if url and st.button("Fetch"):
        if url.lower().endswith(".pdf"):
            response = requests.get(url)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(response.content)
                with pdfplumber.open(tmp_file.name) as pdf:
                    policy_text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
        else:
            page = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(page.text, "html.parser")
            policy_text = soup.get_text()

# --- Process and Display Policy ---
if policy_text:
    st.subheader("📘 Policy Summary by Agent:")
    with st.spinner("Gemma is reading your policy..."):
        prompt = f"""
        You're an insurance assistant AI. Here's a policy document:
        -----
        {policy_text[:5000]}
        -----
        Summarize this in plain English. Include:
        - Type of insurance
        - Key coverage
        - Premium amount
        - Expiry / cutoff date (if any)
        - Renewal or claim process
        """
        response = ollama.chat(
            model="gemma3",
            messages=[{"role": "user", "content": prompt}]
        )
        summary = response['message']['content']
        st.success("✅ Summary:")
        st.write(summary)

    # --- Phase 2: Renewal Handling ---
    st.markdown("---")
    st.subheader("⏰ Renewal Reminder Assistant")

    # Date Extraction - basic fallback if model doesn’t pick one
    expiry_date_input = st.date_input("📅 Enter policy expiry date manually (if not detected):")

    if expiry_date_input:
        days_left = (expiry_date_input - datetime.today().date()).days
        st.info(f"⏳ Days left until expiry: {days_left} days")

        if days_left <= 30:
            st.warning("🚨 Policy is about to expire!")

            renewal_choice = st.radio("🔁 Would you like to renew the policy?", ["1 - Yes", "2 - No"])

            if renewal_choice.startswith("1"):
                card_input = st.text_input("💳 Enter Credit/Debit Card (Format: XXXX-XXXX-XXXX-1234):")
                if card_input:
                    st.success("✅ Renewal initiated using the provided card.")
                    # [Optional] trigger a LangChain or API backend to renew
                else:
                    st.info("💡 Awaiting card details to proceed.")
            else:
                st.info("👍 No renewal action taken.")
