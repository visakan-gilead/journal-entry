from pydantic import BaseModel
from typing import List, Dict, Any, Optional

# Authentication models
class UserSignup(BaseModel):
    username: str
    email: str
    password: str

class UserLogin(BaseModel):
    email: str
    password: str


class ProcessResult(BaseModel):
    message: str
    report_path: str

   
    # Main table data
    report_data: Optional[List[Dict[str, Any]]] = []
   
    # Detailed data for the "View" buttons
    ml_flagged_data: Optional[List[Dict[str, Any]]] = []
    rule_flagged_data: Optional[List[Dict[str, Any]]] = []
    explanations_data: Optional[List[Dict[str, Any]]] = []


class ErrorResponse(BaseModel):
    """Defines the structure of the error response."""
    error: str


class QueryRequest(BaseModel):
    """
    Defines the data structure for the request sent to the chatbot endpoint
    (e.g., /ask-question). It includes the user's query and all contextual data.
    """
    # Contextual data passed from the frontend (derived from ProcessResult)
    flagged_items: List[Dict[str, Any]]
    clean_items: List[Dict[str, Any]]
    ml_flagged: List[Dict[str, Any]]
   
    # Full dataframes (sent as list of dicts for LLM context)
    je_df: List[Dict[str, Any]]
    master_df: List[Dict[str, Any]]
    blackline_df: List[Dict[str, Any]]
   
    # The actual user input
    query: str
    amount_threshold: float = 500000.0
    cutoff_date: str = '2025-06-25'

class ChatQueryRequest(BaseModel):
    query: str
    issue: Optional[str] = None

class ChatMessage(BaseModel):
    user_id: str
    session_id: Optional[str] = None
    conversation_id: Optional[str] = None
    message: str

class NewSessionRequest(BaseModel):
    user_id: str

class FeedbackRequest(BaseModel):
    user_id: str
    question: str
    original_response: Optional[str] = None
    response: Optional[str] = None  # Alternative field name
    rating: int
    feedback_text: Optional[str] = None
    feedback: Optional[str] = None  # Alternative field name
    
    class Config:
        # Allow extra fields that aren't defined
        extra = "allow"
