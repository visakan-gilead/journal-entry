
import streamlit as st
import pandas as pd
import requests
import urllib.parse
import json
import uuid
import html
from typing import Dict, Any, List, Optional
from io import BytesIO
import os
from PIL import Image


# Lazy loading optimization
@st.cache_data(ttl=300)
def load_cached_results():
    """Cache API results to avoid reprocessing"""
    return st.session_state.get('api_response', {})

@st.cache_resource
def get_heavy_imports():
    """Cache heavy imports"""
    from PIL import Image
    return Image

@st.cache_data
def get_chat_manager():
    """Cache chat manager initialization"""
    return None  # Placeholder for chat manager

@st.cache_data
def load_session_conversations(_session_id):
    """Cache conversation loading"""
    try:
        response = requests.get(f"{BASE_API_URL}/session/{_session_id}/conversations")
        if response.status_code == 200:
            return response.json().get("conversations", [])
    except:
        return []
    return []

st.set_page_config(page_title="Anomaly Detection & Analysis", layout="wide")

# --- Initial Custom Styling ---
st.markdown("""
<style>
/* Remove top padding from main container */
.block-container {
    padding-top: 1.5rem !important;
}
/* Target all primary and secondary buttons in the app body */
div[data-testid="stColumn"] button {
    width: 100%;
}
/* Make sidebar success boxes smaller */
section[data-testid="stSidebar"] .stSuccess {
    padding: 0.1rem 0.3rem !important;
    margin: 0.1rem 0 !important;
    font-size: 0.8rem !important;
    min-height: auto !important;
}
section[data-testid="stSidebar"] .stSuccess > div {
    padding: 0 !important;
    margin: 0 !important;
}
/* Ensure the chat history container scrolls nicely */
div.st-emotion-cache-1kyxost.ezrsvp2 {
    max-height: 500px;
    overflow-y: auto;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 10px;
}
/* Chat messages container */
.chat-messages {
    max-height: 400px;
    overflow-y: auto;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 15px;
    margin-bottom: 20px;
    background-color: #f9f9f9;
}
/* Style the main chat header */
h2 {
    color: #007bff;
}
/* Reduce vertical space above the chat input to make it feel more attached to the bottom */
div[data-testid="stForm"] {
    margin-top: -1.5rem !important;
    padding-top: 0;
    border: none !important;
}
/* Ensure the chat box and button span the width correctly */
.chat-container {
    padding-top: 1rem;
    border-top: 2px solid #ddd;
    margin-top: 2rem;
}
</style>
""", unsafe_allow_html=True)


# --- Configuration ---
BASE_API_URL = "http://localhost:8000"
CHAT_QUERY_ENDPOINT = f"{BASE_API_URL}/chat-query/"
DOWNLOAD_URL = f"{BASE_API_URL}/download-report/"
API_URL = "http://localhost:8000/process-files/"

# --- Session State Initialization ---
if 'current_view' not in st.session_state:
    st.session_state['current_view'] = None
if 'uploaded_files' not in st.session_state:
    st.session_state['uploaded_files'] = {}
if 'api_response' not in st.session_state:
    st.session_state['api_response'] = None
if 'processing_complete' not in st.session_state:
    st.session_state['processing_complete'] = False
if 'query_history' not in st.session_state:
    st.session_state['query_history'] = []
if 'conversation_id' not in st.session_state:
    st.session_state['conversation_id'] = None
if 'chat_messages' not in st.session_state:
    st.session_state['chat_messages'] = []
if 'conversation_title' not in st.session_state:
    st.session_state['conversation_title'] = 'Analysis Session Chat'
if 'chat_initialized' not in st.session_state:
    st.session_state['chat_initialized'] = False

# Clear chat on new login
if st.session_state.get('logged_in') and not st.session_state.get('chat_initialized'):
    st.session_state['chat_messages'] = []
    st.session_state['conversation_id'] = None
    st.session_state['conversation_title'] = 'Analysis Session Chat'
    st.session_state['chat_initialized'] = True


# --- API Function for Chat ---

def call_chat_query(query: str) -> Dict[str, Any]:
    """Calls the FastAPI endpoint with the user's question."""
   
    payload = {"query": query}
       
    with st.spinner(f"Asking AI Assistant: '{query}'..."):
        try:
            response = requests.post(CHAT_QUERY_ENDPOINT, json=payload, timeout=60)
           
            if response.status_code == 200:
                return response.json()
            else:
                error_data = {}
                try:
                    error_data = response.json()
                except json.JSONDecodeError:
                    error_message = response.text
               
                # Check for common FastAPI error keys
                error_message = error_data.get('error', error_data.get('detail', response.text))
               
                st.error(f"Chat Query Failed (Status {response.status_code})")
                if error_data:
                    st.json(error_data)
                else:
                    st.code(error_message)

                return {"error": f"API Error: {error_message}"}
               
        except requests.exceptions.ConnectionError:
            st.error(f"Connection Error: Could not connect to FastAPI at {BASE_API_URL}. Ensure the server is running.")
            return {"error": "Connection Error"}
        except Exception as e:
            st.error(f"An unexpected error occurred during chat: {e}")
            return {"error": str(e)}

# --- Helper Functions ---

def read_file_preview(file):
    """Read file content for preview (handles both CSV and XLSX)"""
    file.seek(0)
    if file.name.lower().endswith("csv"):
        return pd.read_csv(file, nrows=5)
    else:
        return pd.read_excel(file, nrows=5)

def show_screenshot(screenshot_path, screenshot_type):
    """Display screenshot in a modal-like expander"""
    Image = get_heavy_imports()  # Lazy load PIL
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    correct_path = os.path.join(parent_dir, screenshot_path)
    
    if os.path.exists(correct_path):
        try:
            image = Image.open(correct_path)
            st.image(image, caption=f"{screenshot_type}: {os.path.basename(screenshot_path)}", use_container_width=True)
        except Exception as e:
            st.error(f"Error loading image: {e}")
    else:
        st.warning(f"Screenshot not found at: {correct_path}")

def toggle_view(view_key: str):
    """Toggles the detail view section open or closed."""
    if st.session_state['current_view'] == view_key:
        st.session_state['current_view'] = None
    else:
        st.session_state['current_view'] = view_key

def render_download_button(report_path: str):
    """Renders download button for the report."""
    if not report_path:
        return
    
    # Check if file exists at the path
    if os.path.exists(report_path):
        try:
            with open(report_path, "rb") as file:
                st.download_button(
                    label="📥 Download Evidence Report",
                    data=file.read(),
                    file_name="evidence_collection_report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary"
                )
        except Exception as e:
            st.error(f"Error reading file: {e}")
    else:
        st.warning("Report file not found.")

@st.dialog("📄 Upload Journal Entry File")
def show_je_dialog():
    sap_file = st.file_uploader("Choose Journal Entry Excel file", type=["csv", "xlsx"], key="sap_upload")
    col_ok, col_cancel = st.columns(2)
    with col_ok:
        if st.button("Upload", type="primary", use_container_width=True):
            if sap_file:
                st.session_state['uploaded_files']['journal_entry'] = sap_file
                st.rerun()
            else:
                st.error("Please select a file first")
    with col_cancel:
        if st.button("Cancel", use_container_width=True):
            st.rerun()

@st.dialog("📊 Upload Blackline File")
def show_bl_dialog():
    bl_file = st.file_uploader("Choose Blackline Excel file", type=["csv", "xlsx"], key="bl_upload")
    col_ok, col_cancel = st.columns(2)
    with col_ok:
        if st.button("Upload", type="primary", use_container_width=True):
            if bl_file:
                st.session_state['uploaded_files']['blackline_entry'] = bl_file
                st.rerun()
            else:
                st.error("Please select a file first")
    with col_cancel:
        if st.button("Cancel", use_container_width=True):
            st.rerun()

@st.dialog("🗂️ Upload Master Data File")
def show_master_dialog():
    master_file = st.file_uploader("Choose Master Data Excel file", type=["csv", "xlsx"], key="master_upload")
    col_ok, col_cancel = st.columns(2)
    with col_ok:
        if st.button("Upload", type="primary", use_container_width=True):
            if master_file:
                st.session_state['uploaded_files']['account_master'] = master_file
                st.rerun()
            else:
                st.error("Please select a file first")
    with col_cancel:
        if st.button("Cancel", use_container_width=True):
            st.rerun()

@st.dialog("💬 Chat Options")
def show_chat_options_dialog(message_index):
    st.write("**Rate this response:**")
    
    rating = st.slider("Rating (1-5):", min_value=1, max_value=5, value=3, key=f"rating_{message_index}")
    
    # Show feedback field, make it required if rating <= 3
    if rating <= 3:
        st.warning("Please provide feedback for ratings of 3 or below")
        feedback = st.text_area("Feedback (Required):", key=f"feedback_{message_index}", placeholder="Please explain what could be improved...")
        feedback_required = True
    else:
        feedback = st.text_area("Feedback (Optional):", key=f"feedback_{message_index}", placeholder="Any additional comments...")
        feedback_required = False
    
    col_submit, col_close = st.columns(2)
    
    with col_submit:
        can_submit = not feedback_required or (feedback_required and feedback.strip())
        if st.button("Submit", type="primary", use_container_width=True, disabled=not can_submit):
            if feedback_required and not feedback.strip():
                st.error("Feedback is required for ratings of 3 or below")
            else:
                # Submit feedback to backend
                if message_index < len(st.session_state['chat_messages']):
                    msg = st.session_state['chat_messages'][message_index]
                    
                    # Handle both tuple and dict formats
                    if isinstance(msg, tuple) and len(msg) >= 2:
                        user_msg, bot_msg = msg[0], msg[1]
                    elif isinstance(msg, dict) and msg.get('role') == 'assistant':
                        # For dict format, we need to find the corresponding user message
                        bot_msg = msg['content']
                        # Look for the previous user message
                        user_msg = "Unknown question"
                        if message_index > 0:
                            prev_msg = st.session_state['chat_messages'][message_index - 1]
                            if isinstance(prev_msg, dict) and prev_msg.get('role') == 'user':
                                user_msg = prev_msg['content']
                    else:
                        st.error("Unable to extract message data for feedback")
                        return  # Exit the function instead of continue
                    
                    user_id = st.session_state.user.get('username', 'anonymous') if st.session_state.get('user') else 'anonymous'
                    
                    feedback_payload = {
                        "user_id": user_id,
                        "question": user_msg,
                        "original_response": bot_msg,
                        "rating": rating,
                        "feedback_text": feedback.strip() if feedback.strip() else None
                    }
                    
                    print(f"DEBUG FRONTEND: Sending feedback with question: '{user_msg[:50]}...'")
                    print(f"DEBUG FRONTEND: Bot message: '{bot_msg[:50]}...'")
                    
                    try:
                        response = requests.post(f"{BASE_API_URL}/feedback", json=feedback_payload)
                        if response.status_code == 200:
                            data = response.json()
                            st.success(f"Rating: {rating}/5 submitted!")
                            if feedback.strip():
                                st.success("Feedback submitted!")
                            
                            # Automatically replace with improved response if generated
                            if data.get('has_improvement') and data.get('improved_response'):
                                # Update message based on format
                                if isinstance(st.session_state['chat_messages'][message_index], tuple):
                                    st.session_state['chat_messages'][message_index] = (user_msg, data['improved_response'])
                                elif isinstance(st.session_state['chat_messages'][message_index], dict):
                                    st.session_state['chat_messages'][message_index]['content'] = data['improved_response']
                                
                                # Update in backend - Skip for now since endpoint doesn't exist
                                # if st.session_state.get('conversation_id'):
                                #     try:
                                #         update_payload = {
                                #             "conversation_id": st.session_state['conversation_id'],
                                #             "message_index": message_index,
                                #             "improved_response": data['improved_response'],
                                #             "user_id": user_id
                                #         }
                                #         requests.post(f"{BASE_API_URL}/update-message", json=update_payload)
                                #     except:
                                #         pass
                                
                                st.success("✨ Response improved and updated!")
                                st.rerun()
                        else:
                            st.error("Failed to submit feedback")
                    except Exception as e:
                        st.error(f"Error submitting feedback: {e}")
    
    with col_close:
        if st.button("Close", use_container_width=True):
            st.rerun()

# --- Sidebar: File Upload Section ---
with st.sidebar:
    st.header("File Upload")
    
    # Journal Entry Button
    je_uploaded = 'journal_entry' in st.session_state['uploaded_files']
    je_label = "✓ Journal Entry" if je_uploaded else "Journal Entry"
    if st.button(je_label, use_container_width=True):
        show_je_dialog()
    
    # Blackline Entry Button
    bl_uploaded = 'blackline_entry' in st.session_state['uploaded_files']
    bl_label = "✓ Blackline Entry" if bl_uploaded else "Blackline Entry"
    if st.button(bl_label, use_container_width=True):
        show_bl_dialog()
    
    # Account Master Button
    master_uploaded = 'account_master' in st.session_state['uploaded_files']
    master_label = "✓ Account Master" if master_uploaded else "Account Master"
    if st.button(master_label, use_container_width=True):
        show_master_dialog()
    
    #st.markdown("---")
    
    # Process button
    all_files_uploaded = len(st.session_state['uploaded_files']) == 3
    
    if all_files_uploaded:
        if st.button("Run Analysis", type="primary", use_container_width=True):
            with st.spinner("Processing files..."):
                api_files = {}
                for key, file_obj in st.session_state['uploaded_files'].items():
                    file_obj.seek(0)
                    api_files[key] = (file_obj.name, file_obj.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                
                try:
                    response = requests.post(API_URL, files=api_files)
                    if response.status_code == 200:
                        st.session_state['api_response'] = response.json()
                        st.session_state['processing_complete'] = True
                        st.success("Analysis complete!")
                        st.rerun()
                    else:
                        st.error(f"Error: {response.json().get('error', 'Unknown error')}")
                except requests.exceptions.ConnectionError:
                    st.error("Connection Error: Ensure FastAPI server is running.")
    else:
        st.info(f"Upload all 3 files ({len(st.session_state['uploaded_files'])}/3)")

#st.markdown("---")

result = st.session_state.get('api_response', {})

# --- Page Layout and Title ---
st.header("Anomaly Detection & Analysis")
#st.markdown("---")

# Show upload status or results
if not st.session_state.get('processing_complete', False):
    st.info("👈 Upload files in the sidebar to begin analysis")
    st.stop()

# --- SECTION 1: DATA TABLES ---
if result.get("report_data"):
   
    col_title, col_button = st.columns([0.7, 0.3])
    report_path = result.get("report_path")

    with col_title:
        st.subheader("Anomaly Report Table")

    with col_button:
        render_download_button(report_path)
           
    try:
        report_df = pd.DataFrame(result["report_data"])
       
        # --- Data Preparation ---
        financial_cols = ["GL_Amount", "Sub_Ledger_Amount", "BlackLine_Balance"]
        for col in financial_cols:
            if col in report_df.columns:
                # Cast and round financial columns
                report_df[col] = pd.to_numeric(report_df[col], errors='coerce').round(2)
       
        # Calculate differences for Financial Details tab
        if all(col in report_df.columns for col in financial_cols):
            report_df['GL_vs_Sub_Ledger_Diff'] = report_df['GL_Amount'] - report_df['Sub_Ledger_Amount']
            report_df['GL_vs_BlackLine_Diff'] = report_df['GL_Amount'] - report_df['BlackLine_Balance']

        # --- Column Configuration ---
        base_column_config = {
            "JE_ID": st.column_config.TextColumn("Journal Entry ID"),
            "Account": st.column_config.NumberColumn("Account", format="%d"),
            "Issues": st.column_config.TextColumn("Flagged Issues"),
            "Posting_Date": st.column_config.DateColumn("Posting Date", format="YYYY-MM-DD"),
            "Reconciliation_Status": st.column_config.TextColumn("Reconciliation Status"),
            "Document_Type": st.column_config.TextColumn("Doc Type"),
            "Is_Manual": st.column_config.CheckboxColumn("Manual_Entry"),
            "JE_Screenshot_Local":st.column_config.TextColumn("JE_Snap"),
            "BlackLine_Screenshot_Local": st.column_config.TextColumn("BL_Snap"),
            "Posting_Time": st.column_config.TimeColumn("Post_Time"),
            "User_ID":st.column_config.TextColumn( "User ID"),
            "error": st.column_config.TextColumn("Error_Status"),
        }
       
        financial_format = {
            col: st.column_config.NumberColumn(col.replace('_', ' '), format="$%.2f")
            for col in financial_cols
        }

        # --- Tab Rendering ---
        tab1, tab2, tab3 = st.tabs(["Overview & Issues", "Financial Data", "Evidence Report"])
       
        with tab1:
            st.markdown("### Overview and Key Flags")
            overview_cols = ["JE_ID", "Account", "Issues", "Posting_Date", "Reconciliation_Status", "Document_Type"]
            overview_config = {k: v for k, v in base_column_config.items() if k in overview_cols}
           
            st.dataframe(
                report_df[overview_cols],
                column_config=overview_config,
                hide_index=True,
                use_container_width=True,
                height=200
            )
           
        with tab2:
            st.markdown("### Financial Data")
            if all(col in report_df.columns for col in financial_cols):
                financial_view_cols = [
                    "JE_ID", *financial_cols, 'GL_vs_Sub_Ledger_Diff', 'GL_vs_BlackLine_Diff'
                ]
                financial_config = {
                    **financial_format,
                    "JE_ID": base_column_config["JE_ID"],
                    'GL_vs_Sub_Ledger_Diff': st.column_config.NumberColumn("GL vs Sub_Ledger Diff", format="$%.2f"),
                    'GL_vs_BlackLine_Diff': st.column_config.NumberColumn("GL vs BlackLine Diff", format="$%.2f"),
                }
                st.dataframe(
                    report_df[financial_view_cols],
                    column_config=financial_config,
                    hide_index=True,
                    use_container_width=True,
                    height=200
                )
            else:
                st.warning("Financial columns not found in report data.")

        with tab3:
            st.markdown("### Evidence Report")
            je_options = ["Select JE_ID"] + report_df['JE_ID'].tolist()
            selected_je = st.selectbox("Select JE_ID to view evidence:", je_options)
            
            if selected_je == "Select JE_ID":
                st.info("Please select a JE_ID to view evidence and analysis details")
            else:
                selected_row = report_df[report_df['JE_ID'] == selected_je].iloc[0]
                
                # Initialize session state for screenshots
                if f'show_je_{selected_je}' not in st.session_state:
                    st.session_state[f'show_je_{selected_je}'] = False
                if f'show_bl_{selected_je}' not in st.session_state:
                    st.session_state[f'show_bl_{selected_je}'] = False
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("👁️ View JE Evidence"):
                        st.session_state[f'show_je_{selected_je}'] = True
                
                with col2:
                    if st.button("👁️ View BL Evidence"):
                        st.session_state[f'show_bl_{selected_je}'] = True
                
                # Display screenshots and flags based on session state
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.session_state.get(f'show_je_{selected_je}', False):
                        je_screenshot = selected_row.get('JE_Screenshot_Local', '')
                        if je_screenshot and je_screenshot != 'N/A':
                            show_screenshot(je_screenshot, "Journal Entry")
                            

                        else:
                            st.info("No JE screenshot available")
                
                with col2:
                    if st.session_state.get(f'show_bl_{selected_je}', False):
                        bl_screenshot = selected_row.get('BlackLine_Screenshot_Local', '')
                        if bl_screenshot and bl_screenshot != 'N/A':
                            show_screenshot(bl_screenshot, "Blackline")
                        else:
                            st.info("No Blackline screenshot available")
            
            # SELECTED JE FLAGS IN 2 COLUMNS
            
            # Get flags for selected JE
            ml_flags = result.get('ml_flagged_data', [])
            rule_flags = result.get('rule_flagged_data', [])
            # explanations = result.get('explanations_data', [])
            
            je_ml_flags = [flag for flag in ml_flags if flag.get('JE_ID') == selected_je]
            je_rule_flags = [flag for flag in rule_flags if flag.get('JE_ID') == selected_je]
            # je_explanations = [exp for exp in explanations if exp.get('JE_ID') == selected_je]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Rule Based Flags")
                if je_rule_flags:
                    with st.expander("View Rule Flags", expanded=False):
                        st.json(je_rule_flags)
                else:
                    st.info("No rule flags for this JE")
            
            with col2:
                st.subheader("ML Based Flags")
                if je_ml_flags:
                    with st.expander("View ML Flags", expanded=False):
                        st.json(je_ml_flags)
                else:
                    st.info("No ML flags for this JE")
            
            # with col3:
            #     st.subheader("Explanations")
            #     if je_explanations:
            #         with st.expander("View Explanations", expanded=False):
            #             st.json(je_explanations)
            #     else:
            #         st.info("No explanations for this JE")
            
            # COMMENTED OUT: Original Analysis Details Section
            # view_config: Dict[str, str] = {
            #     'ml_flagged_data': "ML Based Flags",
            #     'rule_flagged_data': "Rule Based Flags",
            #     'explanations_data': "Explanations"
            # }
            # view_cols = st.columns(len(view_config))
            # for i, (key, title) in enumerate(view_config.items()):
            #     label = f"Hide {title.split()[0]} 🔼" if st.session_state['current_view'] == key else f"View {title.split()[0]} 🔽"
            #     with view_cols[i]:
            #         st.subheader(title)
            #         st.button(label, key=f"btn_view_{key}", on_click=toggle_view, args=(key,), use_container_width=True)

    except Exception as e:
        st.error(f"Error displaying report table: {e}")
else:
    st.info("No report data available for display.")



#st.markdown("---")

# Only render chat if processing is complete
if not st.session_state.get('processing_complete', False):
    st.stop()  # Stop execution here if no data processed

# --- SECTION 3: ENHANCED CHATBOT ASSISTANT ---
st.markdown("""
<div class="chat-container">
    <h3>💬 Chatbot</h3>
    <p>Ask questions about your analysis data and get intelligent responses.</p>
</div>
""", unsafe_allow_html=True)

# Initialize session state for session management
if 'current_session_id' not in st.session_state:
    st.session_state['current_session_id'] = None

# Chat controls in single row with better spacing
col1, col2, col3 = st.columns([0.3, 0.4, 0.3])

with col1:
    st.markdown('<p style="font-size: 0.85rem; color: #666; margin-bottom: 5px;">New Conversation</p>', unsafe_allow_html=True)
    if st.button("New Chat", use_container_width=True, type="secondary"):
        # Reset conversation state for new chat
        st.session_state['conversation_id'] = None
        st.session_state['chat_messages'] = []
        st.session_state['conversation_title'] = 'Analysis Session Chat'
        st.session_state['refresh_conversations'] = True
        st.session_state['conv_selectbox_key'] = str(uuid.uuid4())[:8]
        
        # If no session exists, create one
        if not st.session_state.get('current_session_id') and st.session_state.get('user'):
            user_id = st.session_state.user.get('username', 'unknown')
            try:
                response = requests.post(f"{BASE_API_URL}/create-session", json={"user_id": user_id})
                if response.status_code == 200:
                    st.session_state['current_session_id'] = response.json().get('session_id')
            except:
                pass
        
        # Reset selectbox states to default
        if 'conv_select' in st.session_state:
            del st.session_state['conv_select']
        
        # Clear conversation cache for current session
        if st.session_state.get('current_session_id'):
            cache_key = f"cached_conversations_{st.session_state['current_session_id']}"
            if cache_key in st.session_state:
                del st.session_state[cache_key]
        
        st.rerun()

with col2:
    st.markdown('<p style="font-size: 0.85rem; color: #666; margin-bottom: 5px;">Select Session</p>', unsafe_allow_html=True)
    if st.session_state.get('logged_in') and st.session_state.get('user'):
        user_id = st.session_state.user.get('username', 'unknown')
        
        # Cache sessions to prevent repeated API calls
        if 'cached_sessions' not in st.session_state or st.session_state.get('refresh_sessions', False):
            try:
                sessions_resp = requests.get(f"{BASE_API_URL}/user/{user_id}/sessions")
                if sessions_resp.status_code == 200:
                    st.session_state['cached_sessions'] = sessions_resp.json().get("sessions", [])
                    st.session_state['refresh_sessions'] = False
                else:
                    st.session_state['cached_sessions'] = []
            except:
                st.session_state['cached_sessions'] = []
        
        sessions = st.session_state.get('cached_sessions', [])
        
        if sessions:
            # Get current session ID
            current_session_id = st.session_state.get('current_session_id')
            
            # Filter sessions: include current session OR sessions with conversations
            filtered_sessions = []
            for s in sessions:
                if s.get('session_id') == current_session_id or s.get('conversation_count', 0) > 0:
                    filtered_sessions.append(s)
            
            sessions = filtered_sessions
            
            # Format: "Date Time (X conversations)" with (Current) tag
            session_options = ["Select Session"]
            current_session_id = st.session_state.get('current_session_id')
            
            for i, s in enumerate(sessions):
                # Get conversation count for this session
                conv_count = s.get('conversation_count', 0)
                session_text = f"{s.get('session_start', '')[:19].replace('T', ' ')} ({conv_count} chats)"
                # Mark as Current if it matches the current_session_id in session_state
                if s.get('session_id') == current_session_id:
                    session_text += " (Current)"
                session_options.append(session_text)
            
            current_index = 0
            if st.session_state.get('current_session_id'):
                for i, s in enumerate(sessions):
                    if s.get('session_id') == st.session_state['current_session_id']:
                        current_index = i + 1
                        break
            
            selected_session = st.selectbox("", session_options, index=current_index, key="session_select", label_visibility="collapsed")
            
            if selected_session != "Select Session" and current_index != session_options.index(selected_session):
                session_index = session_options.index(selected_session) - 1
                selected_session_data = sessions[session_index]
                st.session_state['current_session_id'] = selected_session_data.get('session_id')
                st.session_state['conversation_id'] = None
                st.session_state['chat_messages'] = []
                # Clear conversation cache for new session
                cache_key = f"cached_conversations_{selected_session_data.get('session_id')}"
                if cache_key in st.session_state:
                    del st.session_state[cache_key]
                st.rerun()
        else:
            st.selectbox("", ["No sessions found"], disabled=True, label_visibility="collapsed")
    else:
        st.selectbox("", ["Login required"], disabled=True, label_visibility="collapsed")

with col3:
    st.markdown('<p style="font-size: 0.85rem; color: #666; margin-bottom: 5px;">Session Conversations</p>', unsafe_allow_html=True)
    if st.session_state.get('current_session_id') and st.session_state.get('logged_in'):
        # Cache key based on session_id to refresh when session changes
        cache_key = f"cached_conversations_{st.session_state['current_session_id']}"
        
        # Force refresh if session changed or refresh flag is set
        if cache_key not in st.session_state or st.session_state.get('refresh_conversations', False):
            try:
                conv_resp = requests.get(f"{BASE_API_URL}/session/{st.session_state['current_session_id']}/conversations")
                if conv_resp.status_code == 200:
                    st.session_state[cache_key] = conv_resp.json().get("conversations", [])
                    st.session_state['refresh_conversations'] = False
                else:
                    st.session_state[cache_key] = []
            except Exception as e:
                print(f"Error fetching conversations: {e}")
                st.session_state[cache_key] = []
        
        conversations = st.session_state.get(cache_key, [])
        
        if conversations:
            conv_options = ["Select Conversation"] + [conv.get("title", "Analysis Chat") for conv in conversations]
            current_conv_index = 0
            
            if st.session_state.get('conversation_id'):
                for i, conv in enumerate(conversations):
                    if conv.get("conversation_id") == st.session_state['conversation_id']:
                        current_conv_index = i + 1
                        break
            
            conv_key = f"conv_select_{st.session_state.get('conv_selectbox_key', 'default')}"
            selected_conv = st.selectbox("", conv_options, index=current_conv_index, key=conv_key, label_visibility="collapsed")
            
            if selected_conv != "Select Conversation" and current_conv_index != conv_options.index(selected_conv):
                conv_index = conv_options.index(selected_conv) - 1
                selected_conv_data = conversations[conv_index]
                conv_id = selected_conv_data.get("conversation_id")
                
                if conv_id != st.session_state.get('conversation_id'):
                    st.session_state['conversation_id'] = conv_id
                    st.session_state['conversation_title'] = selected_conv
                    
                    # Clear old message cache and force fresh load
                    msg_cache_key = f"cached_messages_{conv_id}"
                    if msg_cache_key in st.session_state:
                        del st.session_state[msg_cache_key]
                    
                    # Load messages from API
                    try:
                        msgs_resp = requests.get(f"{BASE_API_URL}/conversation/{conv_id}")
                        if msgs_resp.status_code == 200:
                            messages_data = msgs_resp.json().get("messages", [])
                            # Convert tuples to dict format with role and content
                            formatted_messages = []
                            for msg in messages_data:
                                formatted_messages.append({'role': 'user', 'content': msg[0]})
                                formatted_messages.append({'role': 'assistant', 'content': msg[1]})
                            st.session_state['chat_messages'] = formatted_messages
                        else:
                            st.session_state['chat_messages'] = []
                    except Exception as e:
                        st.error(f"Error loading messages: {e}")
                        st.session_state['chat_messages'] = []
                    
                    st.rerun()
        else:
            st.selectbox("", ["No conversations in session"], disabled=True, label_visibility="collapsed")
    else:
        st.selectbox("", ["Select session first"], disabled=True, label_visibility="collapsed")

# Display current session and conversation info
if st.session_state.get('current_session_id'):
    session_display = st.session_state.get('conversation_title', 'Analysis Session Chat')
    #st.write(f"**Active:** {session_display}")
    st.caption(f"Session ID: {st.session_state['current_session_id'][:8]}... | Conv ID: {st.session_state.get('conversation_id', 'None')[:8] if st.session_state.get('conversation_id') else 'None'}...")
else:
    st.write("**No active session**")

# Show chat messages in dynamic scrollable container (only if messages exist)
if st.session_state['chat_messages'] or st.session_state.get('processing_message', False):
    with st.container(height=400):
        for i, msg in enumerate(st.session_state['chat_messages']):
            # Handle both tuple and dict formats
            if isinstance(msg, tuple) and len(msg) >= 2:
                # Tuple format: (user_msg, assistant_msg)
                with st.chat_message("user"):
                    st.write(msg[0])
                with st.chat_message("assistant"):
                    col_msg, col_btn = st.columns([0.9, 0.1])
                    with col_msg:
                        st.markdown(msg[1])
                    with col_btn:
                        # Red colored button using HTML
                        button_html = f"""
                        <style>
                        .red-gear-btn {{
                            background-color: #ff4b4b;
                            color: white;
                            border: none;
                            border-radius: 4px;
                            padding: 8px 12px;
                            font-size: 18px;
                            cursor: pointer;
                            transition: all 0.3s;
                        }}
                        .red-gear-btn:hover {{
                            background-color: #ff3333;
                            transform: scale(1.1);
                        }}
                        </style>
                        """
                        st.markdown(button_html, unsafe_allow_html=True)
                        if st.button("⚙️", key=f"chat_options_{i}", help="Rate & provide feedback", type="primary"):
                            show_chat_options_dialog(i)
            elif isinstance(msg, dict) and 'role' in msg:
                # Dict format: {'role': 'user/assistant', 'content': 'message'}
                if msg['role'] == 'user':
                    with st.chat_message("user"):
                        st.write(msg['content'])
                else:  # assistant
                    with st.chat_message("assistant"):
                        col_msg, col_btn = st.columns([0.9, 0.1])
                        with col_msg:
                            st.markdown(msg['content'])
                        with col_btn:
                            # Red colored button using HTML
                            button_html = f"""
                            <style>
                            .red-gear-btn {{
                                background-color: #ff4b4b;
                                color: white;
                                border: none;
                                border-radius: 4px;
                                padding: 8px 12px;
                                font-size: 18px;
                                cursor: pointer;
                                transition: all 0.3s;
                            }}
                            .red-gear-btn:hover {{
                                background-color: #ff3333;
                                transform: scale(1.1);
                            }}
                            </style>
                            """
                            st.markdown(button_html, unsafe_allow_html=True)
                            if st.button("⚙️", key=f"chat_options_dict_{i}", help="Rate & provide feedback", type="primary"):
                                show_chat_options_dialog(i)
        
        # Show typing indicator if processing
        if st.session_state.get('processing_message', False):
            with st.chat_message("assistant"):
                st.write("Generating Response...")

# Chat input form (add spacing only if chat messages exist)
if st.session_state['chat_messages'] or st.session_state.get('processing_message', False):
    st.markdown('<div style="margin-top: 20px;"></div>', unsafe_allow_html=True)
else:
    st.markdown('<div style="margin-top: 10px;"></div>', unsafe_allow_html=True)
with st.form("chat_form", clear_on_submit=True):
    col_input, col_button = st.columns([5, 1])
    
    with col_input:
        user_input = st.text_input(
            "Enter your query:",
            label_visibility="collapsed",
            placeholder="Ask about your analysis data...",
            key="chat_input"
        )
    
    with col_button:
        submitted = st.form_submit_button("Send", type="primary", use_container_width=True)

# Handle form submission
if submitted and user_input.strip():
    # Immediately append user message
    st.session_state['chat_messages'].append({'role': 'user', 'content': user_input.strip()})
    st.session_state['last_user_input'] = user_input.strip()
    st.session_state['processing_message'] = True
    st.rerun()

# Process message if flag is set
if st.session_state.get('processing_message', False):
    user_text = st.session_state.get('last_user_input', '').strip()
    
    if not user_text:
        st.session_state['processing_message'] = False
        st.rerun()
    
    if not st.session_state.get('processing_complete'):
        st.error("Please complete file processing first")
        st.session_state['processing_message'] = False
        st.rerun()
    
    user_id = st.session_state.user.get('username', 'unknown') if st.session_state.get('user') else 'anonymous'
    
    payload = {
        "user_id": user_id,
        "session_id": st.session_state.get('current_session_id'),
        "conversation_id": st.session_state.get('conversation_id'),
        "message": user_text
    }
    
    try:
        response = requests.post(f"{BASE_API_URL}/chat", json=payload, timeout=120)
        if response.status_code == 200:
            data = response.json()
            
            # Update session state
            old_conv_id = st.session_state.get('conversation_id')
            st.session_state['conversation_id'] = data["conversation_id"]
            st.session_state['current_session_id'] = data["session_id"]
            st.session_state['conversation_title'] = data.get("title", st.session_state['conversation_title'])
            
            # Append bot response
            st.session_state['chat_messages'].append({'role': 'assistant', 'content': data["bot_response"]})
            
            # Clear flags
            if 'last_user_input' in st.session_state:
                del st.session_state['last_user_input']
            st.session_state['processing_message'] = False
            
            # Force refresh conversations if new conversation was created
            if old_conv_id != data["conversation_id"]:
                st.session_state['refresh_conversations'] = True
                cache_key = f"cached_conversations_{st.session_state['current_session_id']}"
                if cache_key in st.session_state:
                    del st.session_state[cache_key]
            
            # Force refresh sessions to update conversation counts and current tag
            st.session_state['refresh_sessions'] = True
            if 'cached_sessions' in st.session_state:
                del st.session_state['cached_sessions']
            
            # Show similarity indicator
            if data.get("has_similar_history"):
                st.success("💡 AI learned from similar conversations!")
                print("DEBUG: Similar chat found - chunking validation successful!")
            
            st.rerun()
        else:
            st.session_state['chat_messages'].append({'role': 'assistant', 'content': f"❌ Error: {response.json().get('detail', 'Chat failed')}"})
            st.session_state['processing_message'] = False
            st.rerun()
    except Exception as e:
        st.session_state['chat_messages'].append({'role': 'assistant', 'content': f"❌ Connection error: {e}"})
        st.session_state['processing_message'] = False
        st.rerun()

# Display query history
if st.session_state['query_history']:
    st.markdown("---")
    st.subheader("Query History")
    history_df = pd.DataFrame(st.session_state['query_history'])
    st.dataframe(history_df, use_container_width=True, height=150)