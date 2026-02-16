import os
import json
import uuid
from datetime import datetime
from typing import List, Dict, Optional
from collections import deque
import chromadb
from backend.llm import DatabricksLLM

# Unified data storage
CHAT_DATA_FILE = "unified_chat_data.json"

def ensure_dirs():
    """Ensure output directories exist."""
    os.makedirs(os.path.dirname(CHAT_DATA_FILE) if os.path.dirname(CHAT_DATA_FILE) else ".", exist_ok=True)

def load_unified_chat_data():
    """Load unified chat data from JSON file"""
    try:
        with open(CHAT_DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"conversations": []}
    except json.JSONDecodeError:
        return {"conversations": []}

def save_unified_chat_data(data):
    """Save unified chat data to JSON file"""
    try:
        with open(CHAT_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving chat data: {e}")

class SAPChatSystem:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.client = chromadb.PersistentClient(path="./chroma_db")
        self.conversation_collection = self.client.get_or_create_collection(
            name="unified_conversations",
            embedding_function=chromadb.utils.embedding_functions.DefaultEmbeddingFunction()
        )
        self.llm = DatabricksLLM()
        self.conversation_history = deque(maxlen=3)
        self._load_recent_conversations()
    
    def _load_recent_conversations(self):
        """Load last 3 conversations from unified storage into FIFO deque"""
        try:
            data = load_unified_chat_data()
            user_conversations = [c for c in data.get("conversations", []) if c["user_id"] == self.user_id]
            user_conversations.sort(key=lambda x: x['timestamp'])
            
            # Load last 3 into deque
            for conv in user_conversations[-3:]:
                self.conversation_history.append({
                    'question': conv['question'],
                    'answer': conv['response'],  # Convert 'response' to 'answer'
                    'timestamp': conv['timestamp']
                })
        except Exception as e:
            print(f"Error loading conversations: {e}")
    
    def _chunk_conversation(self, question: str, response: str, timestamp: str, feedback: str = None, improved_response: str = None, max_turns: int = 4, overlap_turns: int = 2) -> List[Dict]:
        """Chunk conversation messages with your provided logic"""
        messages = [
            {"timestamp": timestamp, "speaker": "User", "content": question}
        ]
        
        if response:
            messages.append({"timestamp": timestamp, "speaker": "Assistant", "content": response})
        
        if feedback:
            messages.append({"timestamp": timestamp, "speaker": "User", "content": f"Feedback: {feedback}"})
        
        if improved_response:
            messages.append({"timestamp": timestamp, "speaker": "Assistant", "content": f"Improved: {improved_response}"})
        
        chunks = []
        for i in range(0, len(messages), max_turns - overlap_turns):
            window = messages[i:i + max_turns]
            chunk_text = "\n".join([f"[{msg['timestamp']}] {msg['speaker']}: {msg['content']}" for msg in window])
            chunks.append({
                "text": chunk_text,
                "start_time": window[0]['timestamp'],
                "end_time": window[-1]['timestamp'],
                "participants": list(set(m['speaker'] for m in window))
            })
        return chunks
    
    def store_conversation_in_chromadb(self, question: str, answer: str, rating: int = None, feedback: str = None, improved_response: str = None):
        """Store chunked conversation in ChromaDB collection"""
        timestamp = datetime.utcnow().isoformat()
        chunks = self._chunk_conversation(question, answer, timestamp, feedback, improved_response)
        
        for i, chunk in enumerate(chunks):
            conv_id = f"conv_{timestamp}_{self.user_id}_{str(uuid.uuid4())[:8]}_{i}"
            metadata = {
                "user_id": self.user_id,
                "timestamp": timestamp,
                "start_time": chunk["start_time"],
                "end_time": chunk["end_time"],
                "participants": ",".join(chunk["participants"]),
                "has_feedback": bool(feedback),
                "has_improvement": bool(improved_response)
            }
            
            if rating is not None:
                metadata["rating"] = rating
            
            self.conversation_collection.add(
                documents=[chunk["text"]],
                ids=[conv_id],
                metadatas=[metadata]
            )
    
    def add_to_conversation(self, question: str, answer: str):
        """Add Q&A pair to FIFO deque (automatically removes oldest)"""
        conversation_entry = {
            "question": question,
            "answer": answer,
            "timestamp": datetime.utcnow().isoformat()
        }
        # FIFO deque automatically removes oldest when full
        self.conversation_history.append(conversation_entry)
    
    def find_similar_conversations(self, question: str, top_k: int = 5) -> List[Dict]:
        """Vector-based similarity search for relevant past conversations"""
        try:
            results = self.conversation_collection.query(
                query_texts=[question],
                n_results=min(top_k * 2, 10),
                where={"user_id": self.user_id}
            )
            
            if not results['documents'] or not results['documents'][0]:
                return []
            
            similar_conversations = []
            for i, doc in enumerate(results['documents'][0]):
                metadata = results['metadatas'][0][i]
                lines = doc.split('\n')
                
                # Parse timestamp format: [timestamp] Speaker: content
                question_line = ''
                response_line = ''
                feedback = None
                improved_response = None
                
                for line in lines:
                    if '] User: ' in line and not line.startswith('[') or 'Feedback:' in line:
                        if 'Feedback:' in line:
                            feedback = line.split('Feedback: ')[-1]
                        else:
                            question_line = line.split('] User: ')[-1]
                    elif '] Assistant: ' in line:
                        if 'Improved:' in line:
                            improved_response = line.split('Improved: ')[-1]
                        else:
                            response_line = line.split('] Assistant: ')[-1]
                
                conversation = {
                    'question': question_line,
                    'response': response_line,
                    'rating': metadata.get('rating'),
                    'feedback': feedback,
                    'improved_response': improved_response,
                    'timestamp': metadata['timestamp']
                }
                similar_conversations.append(conversation)
            
            # Sort by rating (good examples first)
            similar_conversations.sort(key=lambda x: x.get('rating') or 0, reverse=True)
            
            # Return good and bad examples
            good_examples = [c for c in similar_conversations if c.get('rating') and c.get('rating') >= 4]
            bad_examples = [c for c in similar_conversations if c.get('rating') and c.get('rating') <= 3]
            
            result = []
            if good_examples:
                result.append(good_examples[0])
            if bad_examples:
                result.append(bad_examples[0])
            
            return result
            
        except Exception as e:
            print(f"Error in similarity search: {e}")
            return []
    

    
    def process_feedback_and_improve(self, question: str, original_response: str, rating: int, feedback_text: str = None) -> str:
        """Process feedback with conversation updates and improved response generation"""
        improved_response = None
        
        # Generate improved response if rating < 4 and feedback provided
        if rating < 4 and feedback_text:
            improved_response = self.get_improved_response(question, original_response, feedback_text)
        
        # Update conversation in FIFO deque
        if self.conversation_history and self.conversation_history[-1]['question'] == question:
            if improved_response:
                self.conversation_history[-1]['answer'] = improved_response
        
        # Update unified storage
        self.update_conversation_with_feedback(question, original_response, rating, feedback_text, improved_response)
        
        # Store in ChromaDB
        self.store_conversation_in_chromadb(question, original_response, rating, feedback_text, improved_response)
        
        return improved_response
    
    def update_conversation_with_feedback(self, question: str, response: str, rating: int, feedback: str = None, improved_response: str = None):
        """Update existing conversation with feedback in unified storage"""
        data = load_unified_chat_data()
        
        # Find and update the most recent matching conversation
        for conv in reversed(data["conversations"]):
            if (conv["user_id"] == self.user_id and 
                conv["question"] == question and 
                conv["response"] == response and 
                conv.get("rating") is None):
                conv["rating"] = rating
                if feedback:
                    conv["feedback"] = feedback
                if improved_response:
                    conv["improved_response"] = improved_response
                break
        
        save_unified_chat_data(data)
    
    def get_improved_response(self, question: str, original_response: str, feedback_text: str) -> str:
        """Generate improved response based on user feedback"""
        prompt = f"""You are an accounts expert. Based on the user's feedback, provide an improved response.

Original Question: {question}

Previous Response: {original_response}

User Feedback: {feedback_text}

Provide an improved answer in plain text format without any markdown formatting, bold text, or special characters. Use simple, clear language:"""
        
        response = self.llm.invoke(prompt)
        # Remove markdown formatting
        improved_text = response.content
        improved_text = improved_text.replace('**', '').replace('*', '').replace('###', '').replace('##', '').replace('#', '')
        improved_text = improved_text.replace('```', '').strip()
        return improved_text

    def add_conversation_to_unified_data(self, question: str, response: str, rating: int = None, feedback: str = None, improved_response: str = None):
        """Add conversation to unified data storage"""
        data = load_unified_chat_data()
        
        conversation_entry = {
            "conversation_id": str(uuid.uuid4()),
            "user_id": self.user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "question": question,
            "response": response,
            "rating": rating,
            "feedback": feedback,
            "improved_response": improved_response
        }
        
        data["conversations"].append(conversation_entry)
        save_unified_chat_data(data)
        return conversation_entry["conversation_id"]
    
    def get_response(self, question: str, additional_context: str = None) -> tuple[str, bool, List[Dict]]:
        """Get response data with similarity examples for services.py"""
        enhanced_question = question
        if additional_context:
            enhanced_question = f"{question}. Additional context: {additional_context}"
        
        similar_conversations = self.find_similar_conversations(question)
        has_similar_history = bool(similar_conversations)
        
        # Format examples for system prompt
        examples = []
        for conv in similar_conversations:
            rating = conv.get('rating', 0)
            example_type = "good" if rating >= 4 else "bad" if rating <= 3 else "neutral"
            
            example = {
                "type": example_type,
                "rating": rating,
                "question": conv['question'],
                "response": conv['response']
            }
            
            # For bad examples, include feedback and improved response if available
            if example_type == "bad":
                if conv.get('feedback'):
                    example["feedback"] = conv['feedback']
                if conv.get('improved_response'):
                    example["improved_response"] = conv['improved_response']
            
            examples.append(example)
        
        return enhanced_question, has_similar_history, examples
    

    
    def process_text_content(self, conversations: List[Dict], max_turns: int = 4, overlap_turns: int = 2) -> str:
        """Process conversations into chunks and store in ChromaDB"""
        total_chunks = 0
        
        for conv in conversations:
            question = conv.get('question', '')
            response = conv.get('response', '')
            feedback = conv.get('feedback')
            improved_response = conv.get('improved_response')
            timestamp = conv.get('timestamp', datetime.utcnow().isoformat())
            
            chunks = self._chunk_conversation(question, response, timestamp, feedback, improved_response, max_turns, overlap_turns)
            
            for i, chunk in enumerate(chunks):
                conv_id = f"bulk_{timestamp}_{self.user_id}_{str(uuid.uuid4())[:8]}_{i}"
                metadata = {
                    "user_id": self.user_id,
                    "timestamp": timestamp,
                    "start_time": chunk["start_time"],
                    "end_time": chunk["end_time"],
                    "participants": ",".join(chunk["participants"]),
                    "has_feedback": bool(feedback),
                    "has_improvement": bool(improved_response)
                }
                
                self.conversation_collection.add(
                    documents=[chunk["text"]],
                    ids=[conv_id],
                    metadatas=[metadata]
                )
                total_chunks += 1
        
        return f"Processed {len(conversations)} conversations into {total_chunks} chunks"

# API Functions for SAP Integration
def create_chat_session(user_id: str) -> SAPChatSystem:
    """Create a new chat session for a user"""
    return SAPChatSystem(user_id)

def get_chat_response(chat_system: SAPChatSystem, question: str, additional_context: str = None) -> tuple[str, bool, List[Dict]]:
    """Get response data with examples for services.py to handle prompting"""
    return chat_system.get_response(question, additional_context)

def process_user_feedback(chat_system: SAPChatSystem, question: str, original_response: str, rating: int, feedback_text: str = None) -> str:
    """Process user feedback and return improved response if needed"""
    return chat_system.process_feedback_and_improve(question, original_response, rating, feedback_text)

def add_user_feedback(chat_system: SAPChatSystem, question: str, response: str, rating: int, feedback_text: str = None, corrected_response: str = None):
    """Add user feedback to the system"""
    chat_system.update_conversation_with_feedback(question, response, rating, feedback_text, corrected_response)



def get_user_conversations(user_id: str) -> List[Dict]:
    """Get all conversations for a specific user"""
    data = load_unified_chat_data()
    return [conversation for conversation in data.get("conversations", []) if conversation["user_id"] == user_id]