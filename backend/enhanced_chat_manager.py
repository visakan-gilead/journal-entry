import chromadb
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import uuid
from collections import deque

class EnhancedChatManager:
    def __init__(self, max_recent_conversations: int = 3):
        self.client = chromadb.PersistentClient(path="./chroma_db")
        self.max_recent_conversations = max_recent_conversations
        
        # Collections for persistent storage
        self.sessions_collection = self.client.get_or_create_collection(
            name="sessions",
            embedding_function=chromadb.utils.embedding_functions.DefaultEmbeddingFunction()
        )
        self.conversations_collection = self.client.get_or_create_collection(
            name="conversations",
            embedding_function=chromadb.utils.embedding_functions.DefaultEmbeddingFunction()
        )
        self.messages_collection = self.client.get_or_create_collection(
            name="messages",
            embedding_function=chromadb.utils.embedding_functions.DefaultEmbeddingFunction()
        )
        
        # In-memory storage for recent conversations per user
        self.recent_conversations: Dict[str, deque] = {}
    
    def _get_user_recent_conversations(self, user_id: str) -> deque:
        """Get or create recent conversations deque for user"""
        if user_id not in self.recent_conversations:
            self.recent_conversations[user_id] = deque(maxlen=self.max_recent_conversations)
        return self.recent_conversations[user_id]
    
    def _move_to_vector_db(self, conversation_data: Dict):
        """Move conversation from memory to vector database"""
        try:
            conversation_id = conversation_data['conversation_id']
            
            # Store conversation metadata
            self.conversations_collection.add(
                documents=[f"Conversation: {conversation_data['title']}"],
                ids=[conversation_id],
                metadatas=[{
                    "session_id": conversation_data['session_id'],
                    "user_id": conversation_data['user_id'],
                    "title": conversation_data['title'],
                    "created_at": conversation_data['created_at'],
                    "message_count": str(len(conversation_data['messages'])),
                    "archived": "true"
                }]
            )
            
            # Store individual messages
            for i, (user_msg, bot_response) in enumerate(conversation_data['messages']):
                message_id = f"{conversation_id}_msg_{i}"
                message_text = f"User: {user_msg}\nBot: {bot_response}"
                
                self.messages_collection.add(
                    documents=[message_text],
                    ids=[message_id],
                    metadatas=[{
                        "conversation_id": conversation_id,
                        "user_message": user_msg,
                        "bot_response": bot_response,
                        "timestamp": conversation_data['created_at'],
                        "message_index": str(i)
                    }]
                )
            
            print(f"Moved conversation {conversation_id} to vector DB")
        except Exception as e:
            print(f"Error moving conversation to vector DB: {e}")
    
    def create_new_session(self, user_id: str) -> str:
        """Create a new session for user"""
        session_id = str(uuid.uuid4())
        session_data = {
            "user_id": user_id,
            "session_start": datetime.utcnow().isoformat(),
            "session_end": "",
            "is_active": "true"
        }
        
        self.sessions_collection.add(
            documents=[f"Session for user {user_id}"],
            ids=[session_id],
            metadatas=[session_data]
        )
        return session_id
    
    def create_new_conversation(self, session_id: str, user_id: str) -> str:
        """Create a new conversation in session"""
        conversation_id = str(uuid.uuid4())
        
        # Store in ChromaDB immediately
        self.conversations_collection.add(
            documents=[f"Conversation in session {session_id}"],
            ids=[conversation_id],
            metadatas=[{
                "session_id": session_id,
                "user_id": user_id,
                "title": "Analysis Session Chat",
                "created_at": datetime.utcnow().isoformat(),
                "message_count": "0",
                "archived": "false"
            }]
        )
        
        # Create conversation data for recent memory (deque for quick access)
        conversation_data = {
            "conversation_id": conversation_id,
            "session_id": session_id,
            "user_id": user_id,
            "title": "Analysis Session Chat",
            "created_at": datetime.utcnow().isoformat(),
            "messages": []
        }
        
        # Add to recent conversations deque
        recent_convs = self._get_user_recent_conversations(user_id)
        
        # If at max capacity, just remove oldest from deque (already in ChromaDB)
        if len(recent_convs) == self.max_recent_conversations:
            recent_convs.popleft()  # Remove oldest from memory only
        
        recent_convs.append(conversation_data)
        return conversation_id
    
    def add_message(self, user_id: str, session_id: str, message: str, response: str) -> str:
        """Add message to conversation - creates new conversation if needed"""
        try:
            # Create new conversation if none exists
            conversation_id = self.create_new_conversation(session_id, user_id)
            
            # Add the message
            message_id = self.append_message(conversation_id, message, response)
            return message_id
        except Exception as e:
            print(f"Error in add_message: {e}")
            return str(uuid.uuid4())
    
    def append_message(self, conversation_id: str, user_message: str, bot_response: str) -> str:
        """Add message pair to conversation"""
        message_id = str(uuid.uuid4())
        
        # Update in recent memory
        for user_id, recent_convs in self.recent_conversations.items():
            for conv in recent_convs:
                if conv['conversation_id'] == conversation_id:
                    conv['messages'].append((user_message, bot_response))
                    
                    # Update ChromaDB with new message count
                    try:
                        self.conversations_collection.update(
                            ids=[conversation_id],
                            metadatas=[{
                                "session_id": conv['session_id'],
                                "user_id": conv['user_id'],
                                "title": conv['title'],
                                "created_at": conv['created_at'],
                                "message_count": str(len(conv['messages'])),
                                "archived": "false"
                            }]
                        )
                    except Exception as e:
                        print(f"Error updating conversation in ChromaDB: {e}")
                    
                    return message_id
        
        # If not in recent memory, update ChromaDB directly
        message_text = f"User: {user_message}\nBot: {bot_response}"
        message_data = {
            "conversation_id": conversation_id,
            "user_message": user_message,
            "bot_response": bot_response,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.messages_collection.add(
            documents=[message_text],
            ids=[message_id],
            metadatas=[message_data]
        )
        
        return message_id
    
    def get_conversation_context(self, user_id: str, conversation_id: str = None) -> List[Tuple[str, str]]:
        """Get conversation context - prioritizes recent conversations"""
        context_messages = []
        
        # First check recent conversations
        recent_convs = self._get_user_recent_conversations(user_id)
        
        if conversation_id:
            # Get specific conversation
            for conv in recent_convs:
                if conv['conversation_id'] == conversation_id:
                    return conv['messages'][-10:]  # Last 10 messages for context
        
        # Get all recent messages for general context
        for conv in list(recent_convs)[-2:]:  # Last 2 conversations
            context_messages.extend(conv['messages'][-3:])  # Last 3 messages each
        
        # If not enough context, search vector DB
        if len(context_messages) < 5:
            try:
                vector_messages = self.get_messages_from_vector_db(conversation_id or "", limit=5)
                context_messages.extend(vector_messages)
            except Exception as e:
                print(f"Error getting vector DB context: {e}")
        
        return context_messages[-10:]  # Return last 10 for context
    
    def get_messages_from_vector_db(self, conversation_id: str, limit: int = 10) -> List[Tuple[str, str]]:
        """Get messages from vector database as tuples"""
        try:
            results = self.messages_collection.get(
                where={"conversation_id": conversation_id} if conversation_id else {},
                limit=limit
            )
            
            messages = []
            if results['metadatas']:
                sorted_metadata = sorted(results['metadatas'], 
                                       key=lambda x: x.get('timestamp', ''))
                for metadata in sorted_metadata:
                    user_msg = metadata.get('user_message', '')
                    bot_response = metadata.get('bot_response', '')
                    messages.append((user_msg, bot_response))
            return messages
        except Exception as e:
            print(f"Error getting messages from vector DB: {e}")
            return []

# Global instance
_chat_manager = None

def get_enhanced_chat_manager() -> EnhancedChatManager:
    """Get or create the global chat manager instance"""
    global _chat_manager
    if _chat_manager is None:
        _chat_manager = EnhancedChatManager()
    return _chat_manager