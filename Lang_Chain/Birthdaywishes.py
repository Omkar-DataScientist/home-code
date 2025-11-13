import streamlit as st
import time
import random

# Set page configuration first
st.set_page_config(
    page_title="ML Birthday Wishes!",
    page_icon="🎂",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# Custom CSS for styling buttons and inputs
st.markdown("""
<style>
.stButton>button {
    background-color: #4CAF50;
    color: white;
    padding: 10px 24px;
    border-radius: 8px;
    border: none;
    cursor: pointer;
    font-size: 16px;
    margin-top: 20px;
}
.stButton>button:hover {
    background-color: #45a049;
}
.stTextInput>div>div>input {
    border-radius: 8px;
    border: 1px solid #ccc;
    padding: 10px;
}
</style>
""", unsafe_allow_html=True)

# Function to generate ML-themed wish
def generate_ml_wish(friend_name):
    """
    Generates a machine learning themed birthday wish.
    """
    ml_phrases = [
        "May your accuracy always be high, and your biases low!",
        "Wishing you a year of optimal parameters and zero overfitting!",
        "May your life's data be perfectly labeled and your features always significant!",
        "Here's to a future with more positive outcomes and fewer false positives!",
        "May your happiness metrics consistently show an upward trend!",
        "Wishing you a year of successful model deployments and abundant insights!",
        "May your journey through life be as smooth as a well-converged gradient descent!",
        "Happy Birthday! May your joy be deep-learned and your celebrations massively parallel!",
        "May your special day be filled with precision, recall, and a high F1-score of happiness!",
        "Wishing you a year where every prediction is true, and every dream is realized!",
        "May your features be engineering gold, and your insights truly novel!",
    ]
    return f"Happy Birthday, {friend_name}! " + random.choice(ml_phrases)

# App title and intro
st.title("🎂 Happy Birthday! Let's ML Some Wishes! 🚀")
st.write("---")
st.write("This app will generate a special birthday wish for your friend, inspired by the wonderful world of Machine Learning!")

# User input
friend_name = st.text_input("Enter your friend's name:", "My ML Buddy")
st.write("---")

# Button action
if st.button("Train My Birthday Model!"):
    if not friend_name.strip():
        st.warning("Please enter a name to generate a personalized wish.")
    else:
        st.info("### **Training the Birthday Model... Please wait!**")
        progress_bar = st.progress(0)
        try:
            for i in range(100):
                time.sleep(0.03)
                progress_bar.progress(i + 1)
        except Exception as e:
            st.error("An error occurred during training: " + str(e))
        else:
            st.success("🎉 Model Training Complete! Birthday Wish Generated! 🎉")
            st.balloons()
            st.write("---")
            st.header(f"✨ {generate_ml_wish(friend_name)} ✨")
            st.write("---")
            st.markdown("""
            **Explanation for the ML-minded:**

            This "model" leverages a highly sophisticated **Random Wish Selector Algorithm** combined with a **Personalized Name Embedding Layer**. It was trained on a diverse dataset of positive affirmations and celebratory sentiments, with a strong focus on maximizing the "Joy Score" metric. While it doesn't utilize neural networks (yet!), its interpretability is 100%! 😉
            """)

# Footer
st.write("---")
st.markdown("Developed with ❤️ for your Special Friend.")
