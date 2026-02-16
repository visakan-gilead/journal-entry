import os #need to work on stop word removal in vectordb
import json
import uuid
from datetime import datetime
from typing import List, Dict, Optional
from collections import deque
import chromadb
import PyPDF2
from llm import DatabricksLLM

# Chat data file
CHAT_DATA_FILE = "chat_system_data.json"

def ensure_dirs():
    """Ensure output directories exist."""
    os.makedirs(os.path.dirname(CHAT_DATA_FILE) if os.path.dirname(CHAT_DATA_FILE) else ".", exist_ok=True)

def load_chat_data():
    """Load chat data from JSON file"""
    try:
        with open(CHAT_DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"conversations": []}
    except json.JSONDecodeError:
        print(f"Warning: Invalid JSON in {CHAT_DATA_FILE}, creating new file")
        return {"conversations": []}

def save_chat_data(data):
    """Save chat data to JSON file"""
    try:
        with open(CHAT_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving chat data: {e}")

def add_conversation_to_chat_data(user_id: str, question: str, response: str, rating: int = None, feedback: str = None, improved_response: str = None):
    """Add conversation to chat data structure"""
    data = load_chat_data()
    
    conversation_entry = {
        "conversation_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "question": question,
        "response": response,
        "rating": rating,
        "feedback": feedback,
        "improved_response": improved_response
    }
    
    data["conversations"].append(conversation_entry)
    save_chat_data(data)
    return conversation_entry["conversation_id"]

class SAPChatSystem:
    def __init__(self, user_id: str, collection_name='sap_knowledge'):
        self.user_id = user_id
        # Initialize ChromaDB collections
        self.client = chromadb.PersistentClient(path="./chroma_db")
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            embedding_function=chromadb.utils.embedding_functions.DefaultEmbeddingFunction()
        )
        # Single conversation collection with all data
        self.conversation_collection = self.client.get_or_create_collection(
            name=f"conversations_{user_id}",
            embedding_function=chromadb.utils.embedding_functions.DefaultEmbeddingFunction()
        )
        self.llm = DatabricksLLM()
        self.conversation_history = deque(maxlen=3)  # FIFO for last 3 conversations
        self._load_recent_conversations_from_json()
    
    def _load_recent_conversations_from_json(self):
        """Load last 3 conversations from JSON into deque and migrate rated conversations to ChromaDB"""
        try:
            data = load_chat_data()
            user_conversations = [c for c in data.get("conversations", []) if c["user_id"] == self.user_id]
            
            # Migrate rated conversations to ChromaDB (avoid duplicates)
            existing_ids = set()
            try:
                existing_results = self.conversation_collection.get()
                existing_ids = set(existing_results['ids']) if existing_results['ids'] else set()
                print(f"DEBUG: Found {len(existing_ids)} existing conversations in ChromaDB")
            except:
                print("DEBUG: No existing conversations in ChromaDB")
                pass
            
            migrated_count = 0
            for conv in user_conversations:
                conv_id = conv.get('conversation_id', f"conv_{conv['timestamp']}")
                if conv.get('rating') is not None and conv_id not in existing_ids:
                    print(f"DEBUG: Migrating conversation with rating {conv.get('rating')}: {conv['question'][:50]}...")
                    self.store_conversation_in_chromadb(
                        conv['question'], 
                        conv['response'], 
                        conv.get('rating'),
                        conv.get('feedback'),
                        conv.get('improved_response')
                    )
                    migrated_count += 1
                elif conv.get('rating') is not None:
                    print(f"DEBUG: Skipping duplicate conversation: {conv['question'][:50]}...")
            
            print(f"DEBUG: Migrated {migrated_count} rated conversations to ChromaDB")
            
            # Sort by timestamp and get last 3
            user_conversations.sort(key=lambda x: x['timestamp'])
            recent_conversations = user_conversations[-3:]
            
            # Load into deque
            for conv in recent_conversations:
                self.conversation_history.append({
                    'question': conv['question'],
                    'answer': conv['response'],
                    'timestamp': conv['timestamp']
                })
                    
        except Exception as e:
            print(f"Error loading conversations from JSON: {e}")
    
    def store_conversation_in_chromadb(self, question: str, answer: str, rating: int = None, feedback: str = None, improved_response: str = None):
        """Store conversation with all data in ChromaDB"""
        conv_text = f"Q: {question}\nA: {answer}"
        
        if feedback:
            conv_text += f"\nFeedback: {feedback}"
        if improved_response:
            conv_text += f"\nImproved: {improved_response}"
        
        conv_id = f"conv_{datetime.utcnow().isoformat()}_{self.user_id}_{str(uuid.uuid4())[:8]}"
        metadata = {
            "user_id": self.user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "has_feedback": bool(feedback),
            "has_improvement": bool(improved_response)
        }
        
        if rating is not None:
            metadata["rating"] = rating
        
        self.conversation_collection.add(
            documents=[conv_text],
            ids=[conv_id],
            metadatas=[metadata]
        )
    
    def add_to_conversation(self, question: str, answer: str):
        """Add Q&A pair to conversation history deque (for recent context only)"""
        conversation_entry = {
            "question": question,
            "answer": answer,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Add to deque (automatically removes oldest if full - FIFO)
        self.conversation_history.append(conversation_entry)
    
    def update_conversation_with_feedback(self, question: str, response: str, rating: int, feedback: str = None, improved_response: str = None):
        """Update existing conversation with feedback instead of creating duplicate"""
        data = load_chat_data()
        
        print(f"DEBUG: Looking for conversation to update: {question[:50]}...")
        
        # Find the most recent conversation with matching question and response
        for i, conv in enumerate(reversed(data["conversations"])):
            if (conv["user_id"] == self.user_id and 
                conv["question"] == question and 
                conv["response"] == response and 
                conv.get("rating") is None):  # Only update if not already rated
                
                print(f"DEBUG: Found matching conversation at index {len(data['conversations']) - 1 - i}, updating with rating {rating}")
                conv["rating"] = rating
                conv["feedback"] = feedback
                conv["improved_response"] = improved_response
                save_chat_data(data)
                return
        
        print(f"DEBUG: No matching conversation found, creating new entry")
        # If no matching conversation found, create new entry (fallback)
        add_conversation_to_chat_data(self.user_id, question, response, rating, feedback, improved_response)
    
    def add_knowledge_base(self, documents: List[str], document_ids: List[str] = None):
        """Add documents to knowledge base"""
        if document_ids is None:
            document_ids = [f"doc_{i}" for i in range(len(documents))]
        
        self.collection.add(
            documents=documents,
            ids=document_ids
        )
        return f"Added {len(documents)} documents to knowledge base"
    
    def extract_pdf_text(self, pdf_path: str) -> str:
        """Extract text from PDF file"""
        try:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                text = ""
                for page in reader.pages:
                    text += page.extract_text() + "\n"
            return text
        except FileNotFoundError:
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        except Exception as e:
            raise Exception(f"Error reading PDF file: {e}")
    
    def process_pdf_file(self, pdf_path: str, max_turns: int = 4, overlap_turns: int = 2):
        """Load PDF file and process into vectors"""
        pdf_text = self.extract_pdf_text(pdf_path)
        return self.process_text_content(pdf_text, max_turns, overlap_turns)
    
    def process_text_content(self, text_content: str, max_turns: int = 4, overlap_turns: int = 2):
        """Process text content into chunks and store in vector database"""
        sentences = [s.strip() for s in text_content.split('.') if s.strip()]
        
        chunks = []
        for i in range(0, len(sentences), max_turns - overlap_turns):
            window = sentences[i:i + max_turns]
            chunk_text = ". ".join(window)
            chunks.append(chunk_text)
        
        for i, chunk in enumerate(chunks):
            print(f"Chunk {i+1}: {chunk[:100]}...")
        
        # Clear existing collection
        try:
            self.client.delete_collection(self.collection.name)
            self.collection = self.client.create_collection(
                name=self.collection.name,
                embedding_function=chromadb.utils.embedding_functions.DefaultEmbeddingFunction()
            )
        except Exception as e:
            print(f"Warning: Could not delete existing collection: {e}")
        
        self.collection.add(
            documents=chunks,
            ids=[f"chunk_{i}" for i in range(len(chunks))]
        )
        return f"Text content processed into {len(chunks)} chunks"
    
    def find_similar_conversations_with_feedback(self, question: str, top_k: int = 5) -> List[Dict]:
        """Find similar conversations using vector search from ChromaDB"""
        try:
            # First, try to get more results to increase chance of finding bad examples
            results = self.conversation_collection.query(
                query_texts=[question],
                n_results=min(top_k * 3, 15),  # Get 3x more results
                where={"user_id": self.user_id}  # Filter by user
            )
            
            print(f"DEBUG: ChromaDB query returned {len(results['documents'][0]) if results['documents'] and results['documents'][0] else 0} results")
            
            if not results['documents'] or not results['documents'][0]:
                return []
            
            # Convert ChromaDB results back to conversation format
            similar_conversations = []
            for i, doc in enumerate(results['documents'][0]):
                metadata = results['metadatas'][0][i]
                # Parse the document text to extract components
                lines = doc.split('\n')
                question_line = lines[0].replace('Q: ', '') if lines else ''
                response_line = lines[1].replace('A: ', '') if len(lines) > 1 else ''
                
                feedback = None
                improved_response = None
                for line in lines[2:]:
                    if line.startswith('Feedback: '):
                        feedback = line.replace('Feedback: ', '')
                    elif line.startswith('Improved: '):
                        improved_response = line.replace('Improved: ', '')
                
                conversation = {
                    'conversation_id': metadata.get('conversation_id', f'conv_{i}'),
                    'question': question_line,
                    'response': response_line,
                    'rating': metadata.get('rating'),
                    'feedback': feedback,
                    'improved_response': improved_response,
                    'timestamp': metadata['timestamp']
                }
                similar_conversations.append(conversation)
            
            # Sort by rating (good examples first) - handle None ratings
            similar_conversations.sort(key=lambda x: x.get('rating') or 0, reverse=True)
            
            # Return good example and bad example
            good_examples = [c for c in similar_conversations if c.get('rating') is not None and c.get('rating') >= 4]
            bad_examples = [c for c in similar_conversations if c.get('rating') is not None and c.get('rating') <= 2]
            
            print(f"DEBUG: Found {len(good_examples)} good examples, {len(bad_examples)} bad examples")
            for conv in similar_conversations:
                print(f"  Rating: {conv.get('rating')}, Question: {conv['question'][:50]}...")
            
            result = []
            if good_examples:
                result.append(good_examples[0]) 
            if bad_examples:
                result.append(bad_examples[0])
            
            # If no bad examples found in similarity search, get some from all user conversations
            if not bad_examples:
                print("DEBUG: No bad examples in similarity results, searching all user conversations...")
                all_user_results = self.conversation_collection.get(
                    where={"user_id": self.user_id}
                )
                if all_user_results['metadatas']:
                    for i, metadata in enumerate(all_user_results['metadatas']):
                        if metadata.get('rating') is not None and metadata.get('rating') <= 2:
                            doc = all_user_results['documents'][i]
                            lines = doc.split('\n')
                            question_line = lines[0].replace('Q: ', '') if lines else ''
                            response_line = lines[1].replace('A: ', '') if len(lines) > 1 else ''
                            
                            bad_conversation = {
                                'conversation_id': all_user_results['ids'][i],
                                'question': question_line,
                                'response': response_line,
                                'rating': metadata.get('rating'),
                                'feedback': metadata.get('feedback'),
                                'improved_response': metadata.get('improved_response'),
                                'timestamp': metadata['timestamp']
                            }
                            result.append(bad_conversation)
                            print(f"DEBUG: Added bad example from all conversations: rating {bad_conversation['rating']}")
                            break
            
            return result
            
        except Exception as e:
            print(f"Error in similarity search: {e}")
            return []
    
    def get_relevant_context(self, question: str, top_k: int = 3) -> List[str]:
        """Get relevant context from knowledge base"""
        try:
            results = self.collection.query(
                query_texts=[question],
                n_results=top_k
            )
            print("Chroma Chunks")
            for x in results['documents']:
                print(f"x:{x}")
            
            return results['documents'][0] if results['documents'] else []
        except Exception as e:
            print(f"Error getting relevant context: {e}")
            return []
    
    def process_feedback_and_improve(self, question: str, original_response: str, rating: int, feedback_text: str = None) -> str:
        """Process feedback and return improved response if rating < 5"""
        # Generate improved response if rating < 5 and feedback provided
        improved_response = None
        if rating < 5 and feedback_text:
            improved_response = self.get_improved_response(question, original_response, feedback_text)
        
        # Update the most recent conversation in deque
        if self.conversation_history and self.conversation_history[-1]['question'] == question:
            # If improved response exists, replace the answer with improved version
            if improved_response:
                self.conversation_history[-1]['answer'] = improved_response
        
        # Update existing conversation in JSON with feedback (don't create new entry)
        self.update_conversation_with_feedback(question, original_response, rating, feedback_text, improved_response)
        
        # Store/update in ChromaDB with feedback
        self.store_conversation_in_chromadb(question, original_response, rating, feedback_text, improved_response)
        
        return improved_response
    
    def get_response(self, question: str, additional_context: str = None) -> tuple[str, bool]:
        """Get response with additional context if provided"""
        enhanced_question = question
        if additional_context:
            enhanced_question = f"{question}. Additional context: {additional_context}"
        
        relevant_context = self.get_relevant_context(enhanced_question)
        similar_conversations = self.find_similar_conversations_with_feedback(question)
        has_similar_history = bool(similar_conversations)
        
        context_text = "\n".join(relevant_context) if relevant_context else "No specific context available."
        
        # SCENARIO 1: No similar conversations found - Basic Q&A
        if not similar_conversations:
            system_prompt = """You are a helpful assistant that answers questions based on provided context and recent conversation history.

CRITICAL GUARDRAILS:
- ONLY use information from the provided context to answer questions
- If the context doesn't contain enough information to answer the question, respond EXACTLY with: "I don't have enough context to answer this question."
- DO NOT use external knowledge or general information not present in the context
- DO NOT make assumptions or provide generic answers

Instructions:
- Answer the question using the information from the context below
- Be concise and accurate
- Quote relevant parts from the context when appropriate"""
            
            # Add recent conversation history for context
            if self.conversation_history:
                system_prompt += "\n\nRecent conversation history:\n"
                for conv in self.conversation_history:
                    system_prompt += f"Q: {conv['question']}\nA: {conv['answer']}\n\n"
            
            prompt = f"""{system_prompt}

Context:
{context_text}

Question: {enhanced_question}

Answer:"""
        
        # SCENARIO 2: Similar conversations found - Learn from examples
        else:
            system_prompt = """You are a helpful assistant. I will provide you with similar questions and their answers, including user feedback and ratings. Learn from these examples to provide the best possible answer.

CRITICAL GUARDRAILS:
- ONLY use information from the provided context to answer questions
- If the context doesn't contain enough information to answer the question, respond EXACTLY with: "I don't have enough context to answer this question."
- DO NOT use external knowledge or general information not present in the context
- DO NOT make assumptions or provide generic answers

Instructions:
- Study the examples below - learn from GOOD examples (high ratings) and avoid mistakes from BAD examples (low ratings)
- For high-rated examples: Follow their approach, style, and completeness
- For low-rated examples: Understand what went wrong from user feedback and avoid those mistakes
- Use improved responses as the best examples when available
- Apply these learnings to answer the current question
- Use the context provided for accurate information"""
            
            # Add similar conversation examples (1 good, 1 bad)
            system_prompt += "\n\nSimilar conversation examples to learn from:\n"
            for i, conv in enumerate(similar_conversations, 1):
                rating = conv.get('rating', 0)
                rating_text = f" (Rating: {rating}/5)"
                example_type = "Good Example" if rating >= 4 else "Bad Example" if rating <= 2 else "EXAMPLE"
                
                system_prompt += f"{example_type}{rating_text}:\n"
                system_prompt += f"Q: {conv['question']}\n"
                system_prompt += f"A: {conv['response']}\n"
                if conv.get('feedback'):
                    system_prompt += f"User Feedback: {conv['feedback']}\n"
                if conv.get('improved_response'):
                    system_prompt += f"Improved Answer: {conv['improved_response']}\n"
                system_prompt += "\n"
            
            prompt = f"""{system_prompt}

Context:
{context_text}

Current Question: {enhanced_question}

Answer:"""
        
        response = self.llm.invoke(prompt)
        
        # Add to conversation history (deque for recent context)
        self.add_to_conversation(enhanced_question, response.content)
        
        # Save conversation to JSON only (ChromaDB storage happens when rated)
        add_conversation_to_chat_data(self.user_id, enhanced_question, response.content)
        
        return response.content, has_similar_history
    
    def get_improved_response(self, question: str, original_response: str, feedback_text: str) -> str:
        """Generate improved response based on user feedback"""
        relevant_context = self.get_relevant_context(question)
        
        # Get similar conversations for learning patterns
        similar_conversations = self.find_similar_conversations_with_feedback(question)
        
        system_prompt = """You are a helpful assistant. The user was not satisfied with your previous response and provided feedback. 
Improve your response based on their feedback while using the provided context.

Provide your improved answer in plain text format without any markdown formatting, bold text, or special characters. Use simple, clear language.

Instructions:
- Address the specific issues mentioned in the user feedback
- Use the context provided to give a more accurate and complete answer
- Be more detailed if feedback indicates insufficient information
- Be more concise if feedback indicates too much information
- Correct any inaccuracies mentioned in the feedback"""
        
        # Add learning examples from similar conversations
        if similar_conversations:
            system_prompt += "\n\nLearn from previous improvements:\n"
            for conv in similar_conversations[:3]:
                if conv.get('feedback') and conv.get('improved_response'):
                    system_prompt += f"Q: {conv['question']}\nFeedback: {conv['feedback']}\nImproved Answer: {conv['improved_response']}\n\n"
        
        context_text = "\n".join(relevant_context) if relevant_context else "No specific context available."
        prompt = f"""{system_prompt}

Context:
{context_text}

Original Question: {question}
Previous Response: {original_response}
User Feedback: {feedback_text}

Improved Answer:"""
        
        response = self.llm.invoke(prompt)
        # Remove markdown formatting
        improved_text = response.content
        improved_text = improved_text.replace('**', '').replace('*', '').replace('###', '').replace('##', '').replace('#', '')
        improved_text = improved_text.replace('```', '').strip()
        return improved_text
    


# API Functions for SAP Integration
def create_chat_session(user_id: str) -> SAPChatSystem:
    """Create a new chat session for a user"""
    return SAPChatSystem(user_id)

def get_chat_response(chat_system: SAPChatSystem, question: str, additional_context: str = None) -> tuple[str, bool]:
    """Get response from chat system with similar history indicator"""
    return chat_system.get_response(question, additional_context)

def process_user_feedback(chat_system: SAPChatSystem, question: str, original_response: str, rating: int, feedback_text: str = None) -> str:
    """Process user feedback and return improved response if needed"""
    improved_response = chat_system.process_feedback_and_improve(question, original_response, rating, feedback_text)
    return improved_response

def add_user_feedback(chat_system: SAPChatSystem, question: str, response: str, rating: int, feedback_text: str = None, corrected_response: str = None):
    """Add user feedback to the system"""
    chat_system.update_conversation_with_feedback(question, response, rating, feedback_text, corrected_response)

def load_pdf_file(chat_system: SAPChatSystem, pdf_path: str):
    """Load PDF file content"""
    return chat_system.process_pdf_file(pdf_path)

def get_chat_data() -> Dict:
    """Get complete chat data for viewing"""
    return load_chat_data()

def get_user_conversations(user_id: str) -> List[Dict]:
    """Get all conversations for a specific user"""
    data = load_chat_data()
    return [conversation for conversation in data.get("conversations", []) if conversation["user_id"] == user_id]


def load_knowledge_base(chat_system: SAPChatSystem, text_content: str):
    """Load knowledge base content"""
    return chat_system.process_text_content(text_content)

# Example usage for testing
if __name__ == "__main__":
    user_id = "newuser60"
    
    # Create chat session
    chat_system = create_chat_session(user_id)
    
    # Load CNN PDF from workspace
    pdf_path = "cnn.pdf"
    if os.path.exists(pdf_path):
        result = load_pdf_file(chat_system, pdf_path)
        print(f"SUCCESS: {result}")
        print("CNN PDF loaded successfully!")
    else:
        print("ERROR: cnn.pdf not found in workspace")
        exit()
    
    # Interactive chat with feedback loop
    print("\nAsk questions about the document.")
    while True:
        question = input("\nEnter your question: ")
        if question.lower() == 'quit':
            break
            
        # Get initial response
        response, has_similar = get_chat_response(chat_system, question)
        print(f"\nAI: {response}")

        
        # Show if similar questions found using cosine similarity
        if has_similar:
            print("\n")
            print("Similar Conversation Found")
            
            # Get and display similar conversations
            similar_convs = chat_system.find_similar_conversations_with_feedback(question)
            if similar_convs:
                for i, conv in enumerate(similar_convs, 1):
                    rating = conv.get('rating', 0)
                    rating_text = f" (Rating: {rating}/5)" if conv.get('rating') else " (No rating)"
                    example_type = "GOOD" if rating >= 4 else "BAD" if rating <= 2 else "NEUTRAL"
                    
                    print(f"\n{i}. {example_type} EXAMPLE{rating_text}: {conv['question']}")
                    print(f"   Answer: {conv['response']}")
                    if conv.get('feedback'):
                        print(f"   Feedback: {conv['feedback']}")
                    if conv.get('improved_response'):
                        print(f"   Improved Response: {conv['improved_response']}")
                    print("\n")
            print("\n")
        
        # Ask if user wants to provide feedback
        feedback_choice = input("\nWould you like to provide feedback on this response? (y/n): ").lower().strip()
        
        if feedback_choice == 'y':
            # Get user rating
            while True:
                try:
                    rating = int(input("Rate this response (1-5): "))
                    if 1 <= rating <= 5:
                        break
                    print("Please enter a number between 1 and 5.")
                except ValueError:
                    print("Please enter a valid number.")
            
            # If rating < 5, ask for feedback and generate improved response
            if rating < 5:
                feedback_text = input("Please provide feedback on how to improve: ")
                if feedback_text:
                    print("\nGenerating improved response based on your feedback...")
                    improved_response = process_user_feedback(chat_system, question, response, rating, feedback_text)
                    if improved_response:
                        print(f"\nImproved AI: {improved_response}")
                        print("SUCCESS: Improved response generated and saved for learning.")
                    else:
                        print("SUCCESS: Feedback saved for learning.")
                else:
                    add_user_feedback(chat_system, question, response, rating)
                    print("SUCCESS: Rating saved.")
            else:
                add_user_feedback(chat_system, question, response, rating, "excellent")
                print("SUCCESS: Great rating! Response saved as a good example.")
        else:
            # No feedback - conversation already saved in get_response()
            print("Conversation saved. Moving to next question.")
        
        print(f"\nLearning Status: {len(get_user_conversations(chat_system.user_id))} total conversations")