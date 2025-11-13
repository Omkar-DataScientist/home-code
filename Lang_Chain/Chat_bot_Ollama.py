import streamlit as st
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.llms import Ollama
import os
from dotenv import load_dotenv

load_dotenv()

# Prompt template
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant. Please respond to the user queries concisely and accurately."),
    ("user", "Question:{question}")
])

def generate_response(question, model_name, temperature, max_tokens):
    try:
        llm = Ollama(model=model_name, temperature=temperature, num_predict=max_tokens)
        output_parser = StrOutputParser()
        chain = prompt | llm | output_parser
        with st.spinner(f"Generating response using {model_name}..."):
            return chain.invoke({'question': question})
    except Exception as e:
        st.error(f"Error: {e}. Is Ollama running? Is model '{model_name}' downloaded?")
        return None

def main():
    st.set_page_config(page_title="Ollama Chatbot", layout="centered")
    st.title("🧠 Ollama-Powered Q&A Chatbot")
    st.markdown("---")

    st.write("""
    This is an interactive chatbot powered by Ollama.

    **Before using, please ensure:**
    1. Ollama is installed and running ([ollama.com](https://ollama.com/))
    2. Your selected model is downloaded, e.g.:
       ```
       ollama run gemma:2b
       ```
    """)

    st.sidebar.header("⚙️ Chatbot Settings")
    llm_model = st.sidebar.selectbox("Model", ["gemma3","gemma:2b", "llama2", "mistral", "phi3"])
    temperature = st.sidebar.slider("Temperature", 0.0, 1.0, 0.7, 0.05)
    max_tokens = st.sidebar.slider("Max Tokens", 50, 1000, 250, 50)

    st.sidebar.markdown("---")
    st.sidebar.info("Choose model and tune response settings.")

    st.subheader("💬 Ask anything below")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_input = st.chat_input("Type your question...")

    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        response = generate_response(user_input, llm_model, temperature, max_tokens)

        if response:
            st.session_state.messages.append({"role": "assistant", "content": response})
            with st.chat_message("assistant"):
                st.markdown(response)

# Required to run Streamlit apps properly
if __name__ == "__main__":
    main()
