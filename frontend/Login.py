import streamlit as st
import requests
import re

# FastAPI backend URL
API_URL = "http://localhost:8000"

# Email validation regex
EMAIL_REGEX = r"[^@]+@[^@]+\.[^@]+"

def is_valid_email(email):
    return re.match(EMAIL_REGEX, email)

def is_strong_password(password):
    # At least 8 characters, one uppercase, one lowercase, one digit
    return len(password) >= 8 and re.search(r"[A-Z]", password) and re.search(r"[a-z]", password) and re.search(r"\d", password)

def signup_user(username, email, password):
    try:
        response = requests.post(f"{API_URL}/signup",
                                 json={"username": username, "email": email, "password": password})
        return response.status_code == 200, response.json()
    except requests.exceptions.RequestException:
        return False, {"detail": "Cannot connect to server"}

def login_user(email, password):
    try:
        response = requests.post(f"{API_URL}/login",
                                 json={"email": email, "password": password})
        return response.status_code == 200, response.json()
    except requests.exceptions.RequestException:
        return False, {"detail": "Cannot connect to server"}

def main():
    st.title("User Authentication System")

    # Initialize session state
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.user = None
        st.session_state.api_response = None
        st.session_state.processing_complete = False
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = 0
    if 'signup_success' not in st.session_state:
        st.session_state.signup_success = False

    if st.session_state.logged_in:
        st.success(f"Welcome, {st.session_state.user['username']}!")
        if st.button("Logout"):
            # Close and cleanup session on logout
            if st.session_state.get('current_session_id'):
                try:
                    # Close session
                    requests.post(f"http://localhost:8000/close-session/{st.session_state['current_session_id']}")
                    # Delete if empty
                    requests.post(f"http://localhost:8000/cleanup-session/{st.session_state['current_session_id']}")
                except:
                    pass
            st.session_state.logged_in = False
            st.session_state.user = None
            st.session_state.api_response = None
            st.session_state.processing_complete = False
            st.session_state.current_session_id = None
            st.session_state.chat_initialized = False
            st.rerun()
    else:
        # Show success message if signup was successful
        if st.session_state.signup_success:
            st.success("Account created successfully! Please login below.")
            st.session_state.signup_success = False
        
        tab1, tab2 = st.tabs(["Login", "Sign Up"])

        with tab1:
            st.header("Login")
            with st.form("login_form"):
                email = st.text_input("Email ID")
                password = st.text_input("Password", type="password")
                submit = st.form_submit_button("Login")

                if submit:
                    if email and password:
                        with st.spinner("Logging in..."):
                            success, response = login_user(email, password)
                        if success:
                            st.session_state.logged_in = True
                            st.session_state.user = response['user']
                            st.session_state.api_response = response
                            st.session_state.processing_complete = True
                            st.session_state.current_session_id = response.get('session_id')  # Use session from login
                            st.success("Login successful! Redirecting...")
                            st.switch_page("pages/Anomaly Reporting.py")
                        else:
                            st.error(response.get('detail', 'Login failed'))
                    else:
                        st.error("Please fill in all fields")

        with tab2:
            st.header("Sign Up")
            with st.form("signup_form"):
                new_username = st.text_input("Username", key="signup_username")
                new_email = st.text_input("Email", key="signup_email")
                new_password = st.text_input("Password", type="password", key="signup_password")
                confirm_password = st.text_input("Confirm Password", type="password")
                submit = st.form_submit_button("Sign Up")

                if submit:
                    if new_username and new_email and new_password and confirm_password:
                        if not is_valid_email(new_email):
                            st.error("Invalid email format")
                        elif not is_strong_password(new_password):
                            st.error("Password must be at least 8 characters long and include uppercase, lowercase, and a digit")
                        elif new_password != confirm_password:
                            st.error("Passwords do not match")
                        else:
                            with st.spinner("Creating account..."):
                                success, response = signup_user(new_username, new_email, new_password)
                            if success:
                                st.session_state.signup_success = True
                                st.rerun()
                            else:
                                st.error(response.get('detail', 'Signup failed'))
                    else:
                        st.error("Please fill in all fields")

if __name__ == "__main__":
    main()