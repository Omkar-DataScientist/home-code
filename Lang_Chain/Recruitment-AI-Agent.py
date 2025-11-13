import streamlit as st
import os
import time
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from langchain_community.llms import Ollama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.tracers.langchain import LangChainTracer

# --- Load environment variables ---
load_dotenv()
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGCHAIN_API_KEY")
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = "Recruitment-AI-Agent"

# --- LangSmith tracer setup ---
tracer = LangChainTracer(project_name=os.environ["LANGCHAIN_PROJECT"])

# --- Prompt template for profile ranking ---
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a recruitment assistant. Rank the visible candidate profiles based on how well they match the job requirement."),
    ("user", """
Requirement:
{requirement}

Candidate Profiles:
{profiles}

Instructions:
1. Select the top 3 best-matching candidates.
2. Give each a score out of 10.
3. Justify briefly why they were selected.
""")
])

# --- Scraper for non-LinkedIn pages (basic HTML scraping) ---
def fetch_search_result_snippets(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")

        profiles = []
        for result in soup.select("a"):
            link = result.get("href")
            name = result.get_text(strip=True)
            if link and name and len(name) > 5 and "http" in link:
                profiles.append({"name": name, "link": link})
        return profiles[:10]
    except Exception as e:
        return [{"name": f"Error fetching from {url}", "link": str(e)}]

# --- Scraper for LinkedIn Connections page using Selenium ---
def fetch_linkedin_connections(url):
    try:
        options = Options()
        options.add_argument("--start-maximized")
        # Disable headless to allow manual login
        driver = webdriver.Chrome(options=options)

        driver.get(url)

        st.info("🔐 Please log in to LinkedIn in the browser window. After login, wait 10 seconds.")
        time.sleep(10)

        # Scroll to load all connections
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        # Wait for connection containers to load
        time.sleep(5)
        profiles = []

        # New method to extract connection names and links
        connection_cards = driver.find_elements(By.CSS_SELECTOR, "li.mn-connection-card")
        for card in connection_cards:
            try:
                name_elem = card.find_element(By.CSS_SELECTOR, "span.mn-connection-card__name")
                link_elem = card.find_element(By.CSS_SELECTOR, "a.mn-connection-card__link")

                name = name_elem.text.strip()
                link = link_elem.get_attribute("href")

                if name and link and "linkedin.com/in/" in link:
                    profiles.append({"name": name, "link": link})
            except Exception:
                continue

        driver.quit()
        return profiles[:10] if profiles else [{"name": "❌ No visible connections found.", "link": "Try logging in or refreshing the page"}]
    except Exception as e:
        return [{"name": "Error fetching LinkedIn data", "link": str(e)}]

    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=options)

        driver.get(url)
        time.sleep(15)  # Allow login and page to load

        # Scroll to load all connections
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        profiles = []
        links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/in/"]')
        for link_elem in links:
            name = link_elem.text.strip()
            href = link_elem.get_attribute("href")
            if name and href and "linkedin.com/in/" in href:
                profiles.append({"name": name, "link": href})

        driver.quit()
        return profiles[:10]
    except Exception as e:
        return [{"name": "Error fetching LinkedIn data", "link": str(e)}]

# --- Format profile dicts for LLM input ---
def evaluate_profiles(requirement, snippets, model="mistral", temperature=0.3, max_tokens=1000):
    llm = Ollama(model=model, temperature=temperature, num_predict=max_tokens)
    chain = prompt | llm | StrOutputParser()
    combined = "\n---\n".join([f"{p['name']} - {p['link']}" for p in snippets])
    return chain.invoke({"requirement": requirement, "profiles": combined}, config={"callbacks": [tracer]})

# --- Streamlit UI starts here ---
st.set_page_config(page_title="Recruitment AI Agent", layout="centered")
st.title("🔍 AI-Powered Candidate Discovery Agent")
st.markdown("Enter job requirement and search result URLs to rank candidates using AI.")

# --- Sidebar model configuration ---
st.sidebar.header("Model Settings")
model = st.sidebar.selectbox("LLM Model", ["mistral", "llama3:8b", "gemma3"])
temperature = st.sidebar.slider("Temperature", 0.0, 1.0, 0.3)
max_tokens = st.sidebar.slider("Max Tokens", 300, 1500, 1000)

# --- User inputs ---
st.subheader("🔗 Paste Search Page Links (LinkedIn / Naukri / Monster)")
link1 = st.text_input("Search Page Link 1")
link2 = st.text_input("Search Page Link 2 (optional)")
link3 = st.text_input("Search Page Link 3 (optional)")

requirement = st.text_area("📋 Enter Job Title / Requirement (e.g., AI Engineer with Python & AWS)", height=150)

# --- Main Action Button ---
if st.button("🔎 Find Matching Candidates"):
    if not requirement or not link1:
        st.warning("Please provide at least one search link and a job requirement.")
    else:
        with st.spinner("🔄 Fetching and analyzing profiles..."):
            urls = [link for link in [link1, link2, link3] if link]
            all_profiles = []

            for url in urls:
                if "linkedin.com" in url:
                    scraped = fetch_linkedin_connections(url)
                else:
                    scraped = fetch_search_result_snippets(url)
                all_profiles.extend(scraped)

        # Show fetched profiles
        if all_profiles:
            st.markdown("### 🔗 Profiles Found:")
            for p in all_profiles:
                st.markdown(f"- [{p['name']}]({p['link']})")

            # Evaluate with LLM
            with st.spinner("🤖 Ranking candidates using AI..."):
                result = evaluate_profiles(requirement, all_profiles, model, temperature, max_tokens)

            st.success("✅ Done! See the ranked list below.")
            st.markdown("### 🏆 Top Candidates:")
            st.markdown(result)
        else:
            st.warning("⚠️ No profiles found on the given search pages.")

        # LangSmith tracing info
        st.markdown("---")
        st.markdown("### 🧠 LangSmith Tracing Info")
        st.info(f"LangSmith project: `{os.getenv('LANGCHAIN_PROJECT')}`. View detailed traces at [smith.langchain.com](https://smith.langchain.com).")

        st.markdown("### ℹ️ Note")
        st.markdown("This is a prototype. LinkedIn scraping requires login and may break if structure changes.")
