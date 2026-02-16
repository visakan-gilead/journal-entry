from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
import traceback
import pandas as pd
import os
from typing import List, Dict, Any
from time import time
from time import time
from backend.services import (
    generate_screenshots_from_xlsx,
    apply_rules,
    anomaly_ml_flag,
    explain_material_amount_deviations,
    answer_followup_questions,
    collect_evidence
)
from backend.database import SnowflakeDB
from backend.models import ProcessResult, ErrorResponse, ChatQueryRequest, UserSignup, UserLogin
from pydantic import BaseModel

# Chatbot Models
class ChatMessage(BaseModel):
    user_id: str
    session_id: str | None
    conversation_id: str | None
    message: str

class NewSessionRequest(BaseModel):
    user_id: str
from backend.utils import create_folder, save_upload_file, read_excel

app = FastAPI(
    title="Journal Entry Analyzer",
    description="FastAPI backend for processing journal entries and generating explanations.",
    version="1.0.0"
)

db = SnowflakeDB()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
PROCESSED_DATA_CACHE = {
    "je_df": pd.DataFrame(),
    "blackline_df": pd.DataFrame(),
    "master_df": pd.DataFrame(),
    "flagged_items": [],
    "clean_items": [],
    "ml_flagged": []
}

SESSION_CACHE = {}

# Rate limiting cache
RATE_LIMIT_CACHE = {}
@app.on_event("startup")
async def startup_event():
    try:
        db.create_users_table()
        db.create_chat_tables()
        print("Database tables initialized successfully")
    except Exception as e:
        print(f"Error initializing database tables: {e}")

@app.post("/signup")
async def signup(user: UserSignup):
    if db.create_user(user.username, user.email, user.password):
        return {"message": "User created successfully"}
    raise HTTPException(status_code=400, detail="Username or email already exists")

@app.post("/login")
async def login(user: UserLogin):
    authenticated_user = db.authenticate_user(user.email, user.password)
    if authenticated_user:
        # Create a session on login
        try:
            session_id = db.create_new_session(authenticated_user['username'])
            return {"message": "Login successful", "user": authenticated_user, "session_id": session_id}
        except Exception as e:
            print(f"Error creating session on login: {e}")
            return {"message": "Login successful", "user": authenticated_user}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/")
async def root():
    return {"message": "User Authentication API"}

@app.post("/process-files/", response_model=ProcessResult, responses={400: {"model": ErrorResponse}})
async def process_files(
    journal_entry: UploadFile = File(..., alias="journal_entry"),
    blackline_entry: UploadFile = File(..., alias="blackline_entry"),
    account_master: UploadFile = File(..., alias="account_master")
):
    # Initialize all result variables
    final_report_df = pd.DataFrame()
    report_data_list: List[Dict[str, Any]] = []
    report_path = ""
    flagged_items: List[Dict[str, Any]] = []
    ml_flagged: List[Dict[str, Any]] = []
    explanations: List[Dict[str, Any]] = []
    clean_items: List[Dict[str, Any]] = [] # Initialize clean_items for the Pydantic model

    # DataFrames initialized here to be accessible in the outer scope
    je_df = pd.DataFrame()
    bl_df = pd.DataFrame()
    am_df = pd.DataFrame()
   
    try:
        temp_dir = "backend/temp_uploads"
        create_folder(temp_dir)

        # File Handling (Save uploaded files to temp and read into DF)
        je_path = save_upload_file(journal_entry, temp_dir)
        bl_path = save_upload_file(blackline_entry, temp_dir)
        am_path = save_upload_file(account_master, temp_dir)

        je_df = read_excel(je_path)
        bl_df = read_excel(bl_path)
        am_df = read_excel(am_path)

        # Service Calls (Calculations for data lists)
        screenshot_data = []
        for je_id in je_df['JE_ID'].unique():
            # NOTE: generate_screenshots_from_xlsx will need access to the saved file paths (je_path, bl_path)
            # if it relies on reading the file data again, or it should take the DFs.
            je_img = generate_screenshots_from_xlsx(je_id, je_df, bl_df, 'journal_entry.xlsx')
            bl_img = generate_screenshots_from_xlsx(je_id, je_df, bl_df, 'blackline_entry.xlsx')
            screenshot_data.append({'JE_ID': je_id, 'Source_file': 'journal_entry.xlsx', 'Local_Path': je_img})
            screenshot_data.append({'JE_ID': je_id, 'Source_file': 'blackline_entry.xlsx', 'Local_Path': bl_img})
        screenshot_df = pd.DataFrame(screenshot_data)

        # Apply rules and flags
        flagged_items, clean_items = apply_rules(je_df, bl_df, am_df, screenshot_df)
        ml_flagged, _, _ = anomaly_ml_flag(je_df, bl_df, am_df)
       
        # Generate explanations
        explanation_result = explain_material_amount_deviations(flagged_items, je_df, am_df, bl_df)
        explanations = explanation_result.get("explanations", [])
       
        PROCESSED_DATA_CACHE["je_df"] = je_df.copy() # Use .copy() to avoid setting-with-copy warnings
        PROCESSED_DATA_CACHE["blackline_df"] = bl_df.copy()
        PROCESSED_DATA_CACHE["master_df"] = am_df.copy()
        PROCESSED_DATA_CACHE["flagged_items"] = flagged_items
        PROCESSED_DATA_CACHE["clean_items"] = clean_items
        PROCESSED_DATA_CACHE["ml_flagged"] = ml_flagged
       
        # Final Report Creation
        final_report_df = pd.DataFrame(flagged_items)
        report_path = collect_evidence(final_report_df.to_dict(orient='records'), explanations)

        # Prepare data for Pydantic model
        if not final_report_df.empty:
            report_data_list = final_report_df.to_dict(orient='records')
       
        # Return Success Result
        return ProcessResult(
            message="Processing complete",
            report_path=report_path,
            report_data=report_data_list,
            ml_flagged_data=ml_flagged,
            rule_flagged_data=flagged_items,
            explanations_data=explanations,
            clean_items=clean_items # Ensure this is returned for the chat payload
        )

    except HTTPException as e:
        # Catch HTTPException explicitly and convert to 400 JSONResponse
        print(f"\n--- CAUGHT HTTP EXCEPTION: {e.detail} ---")
        return JSONResponse(
            status_code=e.status_code,
            content={"error": f"File Processing Error: {e.detail}"}
        )
       
    except Exception as e:
        print("\n--- SERVER TRACEBACK START ---")
        traceback.print_exc()
        print("--- SERVER TRACEBACK END ---\n")
       
        # Return a consistent 400 ErrorResponse for general failures
        return JSONResponse(
            status_code=400,
            content={"error": f"Processing failed: {type(e).__name__} - {str(e)}"}
        )


@app.post("/chat-query/", responses={400: {"model": ErrorResponse}})
async def chat_query(request: ChatQueryRequest):
    """
    Answers follow-up questions or generates focused explanations using the LLM
    based on the last processed dataset.
    """
   
    # 1. Retrieve data from cache
    je_df = PROCESSED_DATA_CACHE.get("je_df")
    blackline_df = PROCESSED_DATA_CACHE.get("blackline_df")
    master_df = PROCESSED_DATA_CACHE.get("master_df")
   
    flagged_items = PROCESSED_DATA_CACHE.get("flagged_items", [])
    clean_items = PROCESSED_DATA_CACHE.get("clean_items", [])
    ml_flagged = PROCESSED_DATA_CACHE.get("ml_flagged", [])
   
    # 2. Validation Check
    if je_df is None or je_df.empty:
        raise HTTPException(
            status_code=400,
            detail="No processed data found. Please run the /process-files/ endpoint first."
        )

    try:
        print(f"\n--- CHAT QUERY DEBUG ---")
        print(f"Query: {request.query}")
        print(f"Issue: {request.issue}")
        print(f"Flagged items count: {len(flagged_items)}")
        print(f"Clean items count: {len(clean_items)}")
        print(f"ML flagged count: {len(ml_flagged)}")
       
        # 3. Call the core function with all required data
        response_data = answer_followup_questions(
            flagged_items=flagged_items,
            clean_items=clean_items,
            ml_flagged=ml_flagged,
            je_df=je_df,
            master_df=master_df,
            blackline_df=blackline_df,
            query=request.query,
            issue=request.issue
        )
       
        print(f"Response data type: {type(response_data)}")
        print(f"Response data: {response_data}")
        print("--- END CHAT QUERY DEBUG ---\n")

        # 4. Return the structured response
        if isinstance(response_data, str) and response_data.startswith("Error:"):
            raise Exception(f"LLM Chat Error: {response_data}")

        return JSONResponse(
            status_code=200,
            content=response_data
        )

    except Exception as e:
        print(f"\n--- CHAT QUERY ERROR ---")
        print(f"Error: {str(e)}")
        print("--- END CHAT QUERY ERROR ---\n")
        
        print("\n--- CHAT QUERY TRACEBACK START ---")
        traceback.print_exc()
        print("--- CHAT QUERY TRACEBACK END ---\n")
       
        return JSONResponse(
            status_code=400,
            content={"error": f"Chat query failed: {type(e).__name__} - {str(e)}"}
        )
@app.get("/debug-cache/")
async def debug_cache():
    """Debug endpoint to check cache status"""
    return {
        "je_df_shape": PROCESSED_DATA_CACHE["je_df"].shape if not PROCESSED_DATA_CACHE["je_df"].empty else "Empty",
        "blackline_df_shape": PROCESSED_DATA_CACHE["blackline_df"].shape if not PROCESSED_DATA_CACHE["blackline_df"].empty else "Empty",
        "master_df_shape": PROCESSED_DATA_CACHE["master_df"].shape if not PROCESSED_DATA_CACHE["master_df"].empty else "Empty",
        "flagged_items_count": len(PROCESSED_DATA_CACHE["flagged_items"]),
        "clean_items_count": len(PROCESSED_DATA_CACHE["clean_items"]),
        "ml_flagged_count": len(PROCESSED_DATA_CACHE["ml_flagged"])
    }

@app.get("/download-report/")
async def download_report(report_path: str):
    """
    Handles the report download link by serving the generated Excel file.
    """
    # Simple check to prevent path traversal
    if '..' in report_path or not os.path.isabs(report_path):
        raise HTTPException(status_code=400, detail="Invalid report path.")
   
    if not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Report file not found.")

    # Return the file as a downloadable response
    return FileResponse(
        path=report_path,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        filename=os.path.basename(report_path)
    )

# Chatbot Endpoints
@app.post("/chat")
async def chat(msg: ChatMessage):
    """Enhanced chat endpoint for analysis session integration"""
    if not msg.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
    # Get user by username
    user = db.get_user_by_username(msg.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_id = user["id"]
    
    # Create session on first chat
    if msg.session_id is None:
        msg.session_id = db.create_new_session(msg.user_id)
        print(f"DEBUG: Created new session {msg.session_id} for user {msg.user_id}")
    
    # Create new conversation if not provided
    if msg.conversation_id is None:
        print(f"Creating new conversation for session: {msg.session_id}, user_id: {user_id}")
        try:
            msg.conversation_id = db.create_new_conversation(msg.session_id, user_id)
            print(f"Created conversation: {msg.conversation_id}")
        except Exception as e:
            print(f"Error creating conversation: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to create conversation: {str(e)}")
    
    # Get bot response using analysis context
    try:
        if PROCESSED_DATA_CACHE.get("je_df") is not None and not PROCESSED_DATA_CACHE["je_df"].empty:
            # Use answer_followup_questions for better analysis-aware responses
            response_data = answer_followup_questions(
                flagged_items=PROCESSED_DATA_CACHE.get('flagged_items', []),
                clean_items=PROCESSED_DATA_CACHE.get('clean_items', []),
                ml_flagged=PROCESSED_DATA_CACHE.get('ml_flagged', []),
                je_df=PROCESSED_DATA_CACHE["je_df"],
                master_df=PROCESSED_DATA_CACHE["master_df"],
                blackline_df=PROCESSED_DATA_CACHE["blackline_df"],
                query=msg.message,
                issue=None
            )
            # Parse structured response from answer_followup_questions
            structured_response = db.parse_llm_response(response_data, msg.message)
            bot_response = f"**Response:** {structured_response['response']}\n\n**Contributing Factors:** {structured_response['contributing_factors']}"
        else:
            raw_response = db.llm_chat(msg.message)
            # Create dict format for consistency
            response_dict = {"query_results": [{"Query": msg.message, "Response": raw_response}]}
            structured_response = db.parse_llm_response(response_dict, msg.message)
            bot_response = f"**Response:** {structured_response['response']}\n\n**Contributing Factors:** {structured_response['contributing_factors']}"
    except Exception as e:
        print(f"Error generating bot response: {e}")
        bot_response = f"I apologize, but I encountered an error processing your message: {str(e)}"
    
    # Save message to database
    try:
        db.append_message(msg.conversation_id, msg.message, bot_response)
        
        # Update title if first message
        messages = db.get_messages(msg.conversation_id)
        if len(messages) == 1:
            db.update_conversation_title(msg.conversation_id, msg.message)
        
        title = db.get_conversation_title(msg.conversation_id)
    except Exception as e:
        print(f"Error saving message to database: {e}")
        title = "Analysis Session Chat"
    
    return {
        "conversation_id": msg.conversation_id,
        "session_id": msg.session_id,
        "title": title,
        "user_message": msg.message,
        "bot_response": bot_response
    }

@app.get("/conversation/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Fetch all messages in a conversation"""
    messages = db.get_messages(conversation_id)
    if not messages:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"conversation_id": conversation_id, "messages": messages}

# Session Management Endpoints
@app.post("/close-session/{session_id}")
async def close_session(session_id: str):
    """Close a session when user logs out"""
    try:
        db.close_session(session_id)
        return {"message": "Session closed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to close session: {str(e)}")

@app.post("/cleanup-session/{session_id}")
async def cleanup_session(session_id: str):
    """Delete session if it has no conversations"""
    try:
        db.delete_empty_session(session_id)
        return {"message": "Session cleanup completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cleanup session: {str(e)}")

@app.get("/user/{user_id}/sessions")
async def get_user_sessions(user_id: str):
    """Get all sessions for a user with rate limiting"""
    # Rate limiting - allow only 1 request per 2 seconds per user
    current_time = time()
    cache_key = f"sessions_{user_id}"
    
    if cache_key in RATE_LIMIT_CACHE:
        last_request_time = RATE_LIMIT_CACHE[cache_key]
        if current_time - last_request_time < 2:  # 2 second cooldown
            print(f"Rate limited request for user {user_id}")
            return {"sessions": []}  # Return empty to break the loop
    
    RATE_LIMIT_CACHE[cache_key] = current_time
    
    try:
        sessions = db.get_user_sessions(user_id)
        print(f"DEBUG: Found {len(sessions)} sessions for user {user_id}")
        return {"sessions": sessions}
    except Exception as e:
        print(f"DEBUG: Error getting sessions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get sessions: {str(e)}")

@app.get("/session/{session_id}/conversations")
async def get_session_conversations(session_id: str):
    """Get all conversations in a session"""
    try:
        conversations = db.get_session_conversations(session_id)
        return {"conversations": conversations}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get conversations: {str(e)}")

@app.get("/user/{username}/last-three")
async def get_user_last_three(username: str):
    """Fetch last 3 conversations for a user by username"""
    user = db.get_user_by_username(username)
    if not user:
        return {"conversations": []}  # Return empty list instead of error for better UX
    
    conversations = db.get_user_last_three_conversations(user["id"])
    return {"conversations": conversations}

@app.post("/create-session")
async def create_session(request: NewSessionRequest):
    """Create a new session for user"""
    try:
        session_id = db.create_new_session(request.user_id)
        return {"session_id": session_id, "message": "Session created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create session: {str(e)}")

@app.post("/reset-tables")
async def reset_tables():
    """Reset chat tables - use only for debugging"""
    try:
        db.create_chat_tables()
        return {"message": "Tables reset successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reset tables: {str(e)}")

@app.get("/debug-sessions/{username}")
async def debug_sessions(username: str):
    """Debug endpoint to check sessions"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("USE DATABASE SAP")
        cursor.execute("USE SCHEMA PUBLIC")
        
        # Get user
        cursor.execute("SELECT id, username FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        
        if not user:
            return {"error": f"User {username} not found"}
        
        # Get sessions
        cursor.execute("SELECT * FROM chat_sessions WHERE user_id = %s", (user[0],))
        sessions = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "user": {"id": user[0], "username": user[1]},
            "sessions": [list(s) for s in sessions]
        }
    except Exception as e:
        return {"error": str(e)}
