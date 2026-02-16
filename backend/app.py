from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
from typing import List, Dict, Any
from collections import deque
from datetime import datetime
from backend.services import (
    generate_screenshots_from_xlsx,
    apply_rules,
    anomaly_ml_flag,
    explain_material_amount_deviations,
    collect_evidence
)
from backend.services_fixed import safe_answer_followup_questions
from backend.database import SnowflakeDB as UserDatabase
from backend.enhanced_chat_manager import get_enhanced_chat_manager
from backend.models import ProcessResult, ErrorResponse, ChatQueryRequest, UserSignup, UserLogin, ChatMessage, NewSessionRequest, FeedbackRequest
from backend.utils import create_folder, save_upload_file, read_excel



app = FastAPI(
    title="Journal Entry Analyzer",
    description="FastAPI backend for processing journal entries and generating explanations.",
    version="1.0.0"
)

db = UserDatabase()
chat_manager = get_enhanced_chat_manager()

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

# Global conversation history storage with deque (max 3 pairs)
CONVERSATION_HISTORY = {}  # user_id -> deque(maxlen=3)

@app.on_event("startup")
async def startup_event():
    try:
        db.create_users_table()
        # Chat tables now handled by ChromaDB
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
        user_id = authenticated_user['username']
        
        # Initialize deque for user and load last 3 conversations
        if user_id not in CONVERSATION_HISTORY:
            CONVERSATION_HISTORY[user_id] = deque(maxlen=3)
            # Load last 3 conversations from database/storage
            try:
                from sap_chat_system_fixed import get_user_conversations
                user_conversations = get_user_conversations(user_id)
                # Get last 3 conversations and add to deque
                recent_conversations = user_conversations[-3:] if user_conversations else []
                for conv in recent_conversations:
                    # Store improved answer if available, otherwise original response
                    answer = conv.get('improved_response') or conv['response']
                    CONVERSATION_HISTORY[user_id].append({
                        'question': conv['question'],
                        'answer': answer
                    })
            except Exception as e:
                print(f"Error loading conversation history: {e}")
        
        # Create a session on login using ChromaDB
        try:
            session_id = chat_manager.create_new_session(user_id)
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
    final_report_df = pd.DataFrame()
    report_data_list: List[Dict[str, Any]] = []
    report_path = ""
    flagged_items: List[Dict[str, Any]] = []
    ml_flagged: List[Dict[str, Any]] = []
    explanations: List[Dict[str, Any]] = []
    clean_items: List[Dict[str, Any]] = []

    je_df = pd.DataFrame()
    bl_df = pd.DataFrame()
    am_df = pd.DataFrame()
   
    try:
        temp_dir = "backend/temp_uploads"
        create_folder(temp_dir)

        je_path = save_upload_file(journal_entry, temp_dir)
        bl_path = save_upload_file(blackline_entry, temp_dir)
        am_path = save_upload_file(account_master, temp_dir)

        je_df = read_excel(je_path)
        bl_df = read_excel(bl_path)
        am_df = read_excel(am_path)

        screenshot_data = []
        for je_id in je_df['JE_ID'].unique():
            je_img = generate_screenshots_from_xlsx(je_id, je_df, bl_df, 'journal_entry.xlsx')
            bl_img = generate_screenshots_from_xlsx(je_id, je_df, bl_df, 'blackline_entry.xlsx')
            screenshot_data.append({'JE_ID': je_id, 'Source_file': 'journal_entry.xlsx', 'Local_Path': je_img})
            screenshot_data.append({'JE_ID': je_id, 'Source_file': 'blackline_entry.xlsx', 'Local_Path': bl_img})
        screenshot_df = pd.DataFrame(screenshot_data)

        flagged_items, clean_items = apply_rules(je_df, bl_df, am_df, screenshot_df)
        ml_flagged, _, _ = anomaly_ml_flag(je_df, bl_df, am_df)
       
        explanation_result = explain_material_amount_deviations(flagged_items, je_df, am_df, bl_df)
        explanations = explanation_result.get("explanations", [])
       
        PROCESSED_DATA_CACHE["je_df"] = je_df.copy()
        PROCESSED_DATA_CACHE["blackline_df"] = bl_df.copy()
        PROCESSED_DATA_CACHE["master_df"] = am_df.copy()
        PROCESSED_DATA_CACHE["flagged_items"] = flagged_items
        PROCESSED_DATA_CACHE["clean_items"] = clean_items
        PROCESSED_DATA_CACHE["ml_flagged"] = ml_flagged
       
        final_report_df = pd.DataFrame(flagged_items)
        report_path = collect_evidence(final_report_df.to_dict(orient='records'), explanations)

        if not final_report_df.empty:
            report_data_list = final_report_df.to_dict(orient='records')
       
        return ProcessResult(
            message="Processing complete",
            report_path=report_path,
            report_data=report_data_list,
            ml_flagged_data=ml_flagged,
            rule_flagged_data=flagged_items,
            explanations_data=explanations,
            clean_items=clean_items
        )

    except HTTPException as e:
        return JSONResponse(
            status_code=e.status_code,
            content={"error": f"File Processing Error: {e.detail}"}
        )
       
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": f"Processing failed: {type(e).__name__} - {str(e)}"}
        )

@app.post("/chat-query/", responses={400: {"model": ErrorResponse}})
async def chat_query(request: ChatQueryRequest):
    je_df = PROCESSED_DATA_CACHE.get("je_df")
    blackline_df = PROCESSED_DATA_CACHE.get("blackline_df")
    master_df = PROCESSED_DATA_CACHE.get("master_df")
   
    flagged_items = PROCESSED_DATA_CACHE.get("flagged_items", [])
    clean_items = PROCESSED_DATA_CACHE.get("clean_items", [])
    ml_flagged = PROCESSED_DATA_CACHE.get("ml_flagged", [])
   
    if je_df is None or je_df.empty:
        raise HTTPException(
            status_code=400,
            detail="No processed data found. Please run the /process-files/ endpoint first."
        )

    try:
        response_data = safe_answer_followup_questions(
            flagged_items=flagged_items,
            clean_items=clean_items,
            ml_flagged=ml_flagged,
            je_df=je_df,
            master_df=master_df,
            blackline_df=blackline_df,
            query=request.query,
            issue=request.issue,
            user_id="system_user"  # Default for chat-query endpoint
        )

        if isinstance(response_data, str) and response_data.startswith("Error:"):
            raise Exception(f"LLM Chat Error: {response_data}")

        return JSONResponse(status_code=200, content=response_data)

    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": f"Chat query failed: {type(e).__name__} - {str(e)}"}
        )

@app.post("/chat")
async def chat(msg: ChatMessage):
    """Enhanced chat endpoint with session-based conversation management"""
    try:
        from sap_chat_system_updated import SAPChatSystem
        
        user_id = msg.user_id
        session_id = msg.session_id
        conversation_id = msg.conversation_id
        
        print(f"\n{'='*80}")
        print(f"[CHAT DEBUG] New chat request")
        print(f"[CHAT DEBUG] User ID: {user_id}")
        print(f"[CHAT DEBUG] Session ID from request: {session_id}")
        print(f"[CHAT DEBUG] Conversation ID from request: {conversation_id}")
        print(f"[CHAT DEBUG] Message: {msg.message}")
        
        # Get processed data
        je_df = PROCESSED_DATA_CACHE.get("je_df")
        blackline_df = PROCESSED_DATA_CACHE.get("blackline_df")
        master_df = PROCESSED_DATA_CACHE.get("master_df")
        flagged_items = PROCESSED_DATA_CACHE.get("flagged_items", [])
        clean_items = PROCESSED_DATA_CACHE.get("clean_items", [])
        ml_flagged = PROCESSED_DATA_CACHE.get("ml_flagged", [])
        
        if je_df is None or je_df.empty:
            return JSONResponse(
                status_code=400,
                content={"error": "No processed data found. Please run file analysis first."}
            )
        
        # Create or get session if not provided or invalid
        if not session_id:
            print(f"[CHAT DEBUG] No session_id provided, creating new session")
            session_id = chat_manager.create_new_session(user_id)
            print(f"[CHAT DEBUG] Created new session: {session_id}")
        
        # Create or get conversation
        if not conversation_id:
            print(f"[CHAT DEBUG] No conversation_id provided, creating new conversation")
            try:
                conversation_id = chat_manager.create_new_conversation(session_id, user_id)
                print(f"[CHAT DEBUG] Created new conversation: {conversation_id}")
            except Exception as e:
                print(f"[CHAT DEBUG] Error creating conversation: {e}")
                print(f"[CHAT DEBUG] Session might be invalid, creating new session and conversation")
                session_id = chat_manager.create_new_session(user_id)
                conversation_id = chat_manager.create_new_conversation(session_id, user_id)
                print(f"[CHAT DEBUG] New session: {session_id}, conversation: {conversation_id}")
        
        # Get conversation context
        try:
            context_messages = chat_manager.get_conversation_context(user_id, conversation_id)
            print(f"[CHAT DEBUG] Retrieved {len(context_messages)} context messages")
        except Exception as e:
            print(f"[CHAT DEBUG] Error getting context: {e}, using empty context")
            context_messages = []
        
        # Format context for LLM
        conversation_history = [{'question': q, 'answer': a} for q, a in context_messages]
        print(f"[CHAT DEBUG] Formatted conversation history: {len(conversation_history)} entries")
        if conversation_history:
            for i, conv in enumerate(conversation_history[-3:], 1):
                print(f"[CHAT DEBUG]   History {i}: Q='{conv['question'][:50]}...' A='{conv['answer'][:50]}...'")
        
        # Get LLM response
        print(f"[CHAT DEBUG] Calling LLM with conversation history...")
        response_data = safe_answer_followup_questions(
            flagged_items=flagged_items,
            clean_items=clean_items,
            ml_flagged=ml_flagged,
            je_df=je_df,
            master_df=master_df,
            blackline_df=blackline_df,
            query=msg.message,
            issue="General Query",
            user_id=user_id,
            conversation_history=conversation_history
        )
        
        # Extract response text
        if isinstance(response_data, dict) and "query_results" in response_data:
            query_results = response_data["query_results"]
            response_text = query_results[0].get("Response", "No response") if query_results else "No response"
        else:
            response_text = str(response_data)
        
        print(f"[CHAT DEBUG] LLM Response (first 100 chars): {response_text[:100]}...")
        
        # Generate conversation title from first message
        conversation_title = "Analysis Session Chat"
        try:
            # Get current conversation from recent memory
            for uid, recent_convs in chat_manager.recent_conversations.items():
                if uid == user_id:
                    for conv in recent_convs:
                        if conv['conversation_id'] == conversation_id:
                            # If this is the first message, generate title
                            if len(conv['messages']) == 0:
                                # Generate title from question (first 50 chars)
                                conversation_title = msg.message[:50] + "..." if len(msg.message) > 50 else msg.message
                                conv['title'] = conversation_title
                            break
        except Exception as e:
            print(f"[CHAT DEBUG] Error generating title: {e}")
        
        # Store message in chat manager
        print(f"[CHAT DEBUG] Storing in chat_manager...")
        try:
            chat_manager.append_message(conversation_id, msg.message, response_text)
            print(f"[CHAT DEBUG] Stored in chat_manager successfully")
        except Exception as e:
            print(f"[CHAT DEBUG] Error storing in chat_manager: {e}")
        
        # Store in unified data only (ChromaDB storage happens only with feedback)
        print(f"[CHAT DEBUG] Storing in unified_chat_data.json...")
        try:
            from sap_chat_system_updated import SAPChatSystem
            chat_system = SAPChatSystem(user_id)
            chat_system.add_conversation_to_unified_data(msg.message, response_text)
            print(f"[CHAT DEBUG] Stored successfully")
        except Exception as e:
            print(f"[CHAT DEBUG] Error storing: {e}")
        
        print(f"[CHAT DEBUG] Chat completed successfully")
        print(f"{'='*80}\n")
        
        return JSONResponse(status_code=200, content={
            "bot_response": response_text,
            "session_id": session_id,
            "conversation_id": conversation_id,
            "user_id": user_id,
            "title": conversation_title,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"[CHAT DEBUG ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        print(f"{'='*80}\n")
        return JSONResponse(
            status_code=500,
            content={"error": f"Chat processing failed: {str(e)}"}
        )


@app.post("/test-chat")
async def test_chat():
    """Simple test endpoint for chatbot functionality"""
    try:
        from backend.llm import LLM_Chat
        
        llm = LLM_Chat()
        if llm is None:
            return {"error": "Failed to initialize LLM"}
        
        response = llm.invoke("Hello, please respond with 'Chat is working!'")
        return {
            "status": "success",
            "response": response.content if hasattr(response, 'content') else str(response)
        }
    except Exception as e:
        return {"error": f"Test failed: {str(e)}"}

@app.post("/chat/new-session")
async def create_chat_session(request: NewSessionRequest):
    """Create a new chat session for a user"""
    try:
        user_id = request.user_id
        
        print(f"[SESSION DEBUG] Creating new session for user: {user_id}")
        
        # Create session using chat_manager
        session_id = chat_manager.create_new_session(user_id)
        
        print(f"[SESSION DEBUG] Session created: {session_id}")
        
        return JSONResponse(status_code=200, content={
            "session_id": session_id,
            "user_id": user_id,
            "created_at": datetime.now().isoformat(),
            "status": "active"
        })
        
    except Exception as e:
        print(f"[SESSION DEBUG ERROR] {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to create session: {str(e)}"}
        )

@app.get("/chat/session/{user_id}")
async def get_user_session(user_id: str):
    """Get or create session for user"""
    try:
        print(f"[SESSION DEBUG] Getting session for user: {user_id}")
        
        # Create new session (or you can implement logic to retrieve existing active session)
        session_id = chat_manager.create_new_session(user_id)
        
        print(f"[SESSION DEBUG] Session ID: {session_id}")
        
        return JSONResponse(status_code=200, content={
            "session_id": session_id,
            "user_id": user_id,
            "status": "active"
        })
        
    except Exception as e:
        print(f"[SESSION DEBUG ERROR] {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to get session: {str(e)}"}
        )

@app.get("/user/{user_id}/sessions")
async def get_user_sessions(user_id: str):
    """Get all sessions for a user"""
    try:
        print(f"\n[GET SESSIONS] Fetching sessions for user: {user_id}")
        
        results = chat_manager.sessions_collection.get(
            where={"user_id": user_id}
        )
        
        print(f"[GET SESSIONS] Found {len(results['ids']) if results['ids'] else 0} sessions in ChromaDB")
        
        sessions = []
        if results['ids']:
            for i, session_id in enumerate(results['ids']):
                metadata = results['metadatas'][i]
                
                # Count conversations from ChromaDB
                try:
                    conv_results = chat_manager.conversations_collection.get(
                        where={
                            "$and": [
                                {"session_id": session_id},
                                {"archived": "false"}
                            ]
                        }
                    )
                    conv_count = len(conv_results['ids']) if conv_results['ids'] else 0
                except Exception as e:
                    print(f"[GET SESSIONS] Error querying conversations: {e}")
                    # Fallback: count all conversations for this session
                    conv_results = chat_manager.conversations_collection.get(
                        where={"session_id": session_id}
                    )
                    conv_count = len(conv_results['ids']) if conv_results['ids'] else 0
                
                print(f"[GET SESSIONS] Session {session_id[:8]}... has {conv_count} conversations")
                
                sessions.append({
                    "session_id": session_id,
                    "session_start": metadata.get('session_start', ''),
                    "is_active": metadata.get('is_active', 'false'),
                    "conversation_count": conv_count
                })
        
        # Sort by session_start descending (newest first)
        sessions.sort(key=lambda x: x['session_start'], reverse=True)
        
        print(f"[GET SESSIONS] Returning {len(sessions)} sessions\n")
        return JSONResponse(status_code=200, content={"sessions": sessions})
        
    except Exception as e:
        print(f"[GET SESSIONS ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/session/{session_id}/conversations")
async def get_session_conversations(session_id: str):
    """Get all conversations in a session"""
    try:
        conversations = []
        
        # Check recent conversations first
        for user_id, recent_convs in chat_manager.recent_conversations.items():
            for conv in recent_convs:
                if conv.get('session_id') == session_id:
                    conversations.append({
                        "conversation_id": conv['conversation_id'],
                        "title": conv['title'],
                        "created_at": conv['created_at'],
                        "message_count": len(conv['messages']),
                        "session_id": conv['session_id']
                    })
        
        # Get archived conversations from vector DB
        try:
            results = chat_manager.conversations_collection.get(
                where={"session_id": session_id}
            )
            
            if results['ids']:
                for i, conv_id in enumerate(results['ids']):
                    metadata = results['metadatas'][i]
                    # Avoid duplicates
                    if not any(c['conversation_id'] == conv_id for c in conversations):
                        conversations.append({
                            "conversation_id": conv_id,
                            "title": metadata.get('title', 'Analysis Chat'),
                            "created_at": metadata.get('created_at', ''),
                            "message_count": int(metadata.get('message_count', 0)),
                            "session_id": metadata.get('session_id', '')
                        })
        except Exception as e:
            print(f"[GET CONVERSATIONS] Error querying vector DB: {e}")
        
        # Sort by created_at descending
        conversations.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        print(f"[GET CONVERSATIONS] Found {len(conversations)} conversations for session {session_id}")
        
        return JSONResponse(status_code=200, content={"conversations": conversations})
        
    except Exception as e:
        print(f"[GET CONVERSATIONS ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/conversation/{conversation_id}")
async def get_conversation_messages(conversation_id: str):
    """Get all messages in a conversation"""
    try:
        print(f"[GET MESSAGES] Fetching messages for conversation: {conversation_id}")
        
        # Check recent conversations first
        for user_id, recent_convs in chat_manager.recent_conversations.items():
            for conv in recent_convs:
                if conv['conversation_id'] == conversation_id:
                    print(f"[GET MESSAGES] Found in recent: {len(conv['messages'])} messages")
                    return JSONResponse(status_code=200, content={
                        "messages": conv['messages'],
                        "title": conv['title'],
                        "session_id": conv.get('session_id', '')
                    })
        
        # Get from vector DB if not in recent
        print(f"[GET MESSAGES] Not in recent, checking vector DB...")
        messages = chat_manager.get_messages_from_vector_db(conversation_id)
        print(f"[GET MESSAGES] Found in vector DB: {len(messages)} messages")
        
        return JSONResponse(status_code=200, content={
            "messages": messages,
            "title": "Analysis Chat",
            "session_id": ""
        })
        
    except Exception as e:
        print(f"[GET MESSAGES ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})
@app.post("/feedback")
async def update_feedback(feedback: FeedbackRequest):
    """Process user feedback with rating and generate improved response if needed"""
    try:
        from sap_chat_system_updated import SAPChatSystem
        
        user_id = feedback.user_id
        question = feedback.question
        # Handle both field name variations
        original_response = feedback.original_response or feedback.response or ""
        rating = feedback.rating
        feedback_text = feedback.feedback_text or feedback.feedback
        
        print(f"\n{'='*80}")
        print(f"[FEEDBACK DEBUG] Feedback received")
        print(f"[FEEDBACK DEBUG] User ID: {user_id}")
        print(f"[FEEDBACK DEBUG] Rating: {rating}")
        print(f"[FEEDBACK DEBUG] Question: {question[:100] if question else 'None'}...")
        print(f"[FEEDBACK DEBUG] Original response length: {len(original_response) if original_response else 0}")
        print(f"[FEEDBACK DEBUG] Feedback text: {feedback_text}")
        print(f"[FEEDBACK DEBUG] Will generate improved response: {rating < 4 and bool(feedback_text)}")
        
        if not original_response:
            print(f"[FEEDBACK DEBUG ERROR] No original_response provided")
            return JSONResponse(
                status_code=400,
                content={"error": "original_response or response field is required"}
            )
        
        # Create chat system instance
        chat_system = SAPChatSystem(user_id)
        
        # Process feedback and get improved response if rating < 4
        print(f"[FEEDBACK DEBUG] Processing feedback...")
        improved_response = chat_system.process_feedback_and_improve(
            question=question,
            original_response=original_response,
            rating=rating,
            feedback_text=feedback_text
        )
        
        if improved_response:
            print(f"[FEEDBACK DEBUG] Improved response generated!")
            print(f"[FEEDBACK DEBUG] Improved response (first 200 chars): {improved_response[:200]}...")
            print(f"[FEEDBACK DEBUG] Improved response length: {len(improved_response)}")
        else:
            print(f"[FEEDBACK DEBUG] No improved response generated")
            if rating >= 4:
                print(f"[FEEDBACK DEBUG] Reason: Rating is {rating} (>= 4)")
            if not feedback_text:
                print(f"[FEEDBACK DEBUG] Reason: No feedback text provided")
        
        print(f"[FEEDBACK DEBUG] Feedback stored successfully")
        print(f"{'='*80}\n")
        
        response_data = {
            "message": "Feedback processed successfully",
            "improved_response": improved_response,
            "rating": rating,
            "has_improvement": bool(improved_response),
            "original_response": original_response[:100] + "..." if len(original_response) > 100 else original_response
        }
        
        print(f"[FEEDBACK DEBUG] Returning response: {response_data}")
        
        return JSONResponse(status_code=200, content=response_data)
        
    except Exception as e:
        print(f"[FEEDBACK DEBUG ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        print(f"{'='*80}\n")
        return JSONResponse(
            status_code=500,
            content={"error": f"Feedback processing failed: {str(e)}"}
        )

@app.post("/update-message")
async def update_message(data: dict):
    """Update a message in conversation with improved response"""
    try:
        conversation_id = data.get('conversation_id')
        message_index = data.get('message_index')
        improved_response = data.get('improved_response')
        user_id = data.get('user_id')
        
        if not all([conversation_id, improved_response, user_id]):
            return JSONResponse(
                status_code=400,
                content={"error": "Missing required fields"}
            )
        
        # Update in recent conversations
        for uid, recent_convs in chat_manager.recent_conversations.items():
            if uid == user_id:
                for conv in recent_convs:
                    if conv['conversation_id'] == conversation_id:
                        if message_index < len(conv['messages']):
                            user_msg, _ = conv['messages'][message_index]
                            conv['messages'][message_index] = (user_msg, improved_response)
                            return JSONResponse(status_code=200, content={
                                "message": "Message updated successfully"
                            })
        
        return JSONResponse(
            status_code=404,
            content={"error": "Conversation not found in recent messages"}
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Update failed: {str(e)}"}
        )