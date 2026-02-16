import json
from langchain_core.prompts import ChatPromptTemplate
try:
    from .llm import DatabricksLLM
except ImportError:
    from backend.llm import LLM_Chat

def parse_json_response(response_content):
    try:
        return json.loads(response_content)
    except:
        try:
            import re
            json_match = re.search(r'```json\s*({.*})\s*```', response_content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            start = response_content.find('{')
            if start != -1:
                brace_count = 0
                for i, char in enumerate(response_content[start:], start):
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            return json.loads(response_content[start:i+1])
        except Exception as e:
            print(f"JSON parsing error: {e}")
        return {"error": f"Failed to parse JSON: Could not extract valid JSON from response"}

def LLM_Chat():
    try:
        try:
            llm = DatabricksLLM()
            return llm
        except (NameError, ImportError):
            from backend.llm import LLM_Chat as OldLLM
            return OldLLM()
    except Exception as e:
        return f"error: {e}"

def json_dumps(obj, max_depth=3, current_depth=0):
    """Safely convert objects to JSON string, preventing recursion"""
    if current_depth > max_depth:
        return "<max_depth_reached>"
    
    try:
        if isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        elif isinstance(obj, dict):
            return {k: json_dumps(v, max_depth, current_depth + 1) for k, v in list(obj.items())[:10]}
        elif isinstance(obj, (list, tuple)):
            return [json_dumps(item, max_depth, current_depth + 1) for item in obj[:10]]
        else:
            return str(obj)[:100]
    except:
        return "<serialization_error>"

def dataframe_for_json(df, max_rows=None):
    """Convert DataFrame to safe JSON format"""
    if df.empty:
        return []
    
    limited_df = df.head(max_rows).copy() if max_rows else df.copy()
    
    for col in limited_df.columns:
        if limited_df[col].dtype == 'object':
            limited_df[col] = limited_df[col].astype(str)
        elif 'datetime' in str(limited_df[col].dtype):
            limited_df[col] = limited_df[col].astype(str)
    
    return limited_df.to_dict('records')

def answer_followup_questions_simple(flagged_items, clean_items, ml_flagged, je_df, master_df, blackline_df, query=None, issue='Amount Exceeding Thresholds',
amount_threshold=500000, cutoff_date='2025-06-25', conversation_history=None):
    """Simplified version that prevents recursion errors"""
    llm = LLM_Chat()
    try:
        # Safe data conversion
        je_df_json = dataframe_for_json(je_df)
        blackline_df_json = dataframe_for_json(blackline_df)
        master_df_json = dataframe_for_json(master_df)
        
        # Pass all items to LLM - no artificial limits
        safe_flagged = [json_dumps(item) for item in (flagged_items if flagged_items else [])]
        safe_clean = [json_dumps(item) for item in (clean_items if clean_items else [])]
        safe_ml = [json_dumps(item) for item in (ml_flagged if ml_flagged else [])]

        prompt_template = ChatPromptTemplate.from_template("""
        You are an accounts expert analyzing journal entries in SAP/BlackLine.
        
        Answer the user query based on this data:
        - Flagged Items: {flagged_item}
        - Clean Items: {clean_item}
        - ML Flagged: {anomaly_item}
        - JE Details: {je_df}
        - Master: {master_df}
        - Reconciliation: {reconciliation_df}
        
        Current User Query: {user_query}
        
        Return JSON with this EXACT structure:
        {{
          "query_results": [{{
            "Response": "Your natural language answer here",
            "Contributing_Factors": "Comma-separated factors like: Amount Threshold, Manual Entry, Reconciliation Issue",
            "Relevant_JE_IDs": "Comma-separated JE IDs if specific data is requested"
          }}]
        }}
        """)
        
        prompt = prompt_template.format(
            flagged_item=json.dumps(safe_flagged, default=str),
            clean_item=json.dumps(safe_clean, default=str),
            anomaly_item=json.dumps(safe_ml, default=str),
            je_df=json.dumps(je_df_json, default=str),
            master_df=json.dumps(master_df_json, default=str),
            reconciliation_df=json.dumps(blackline_df_json, default=str),
            user_query=query if query else "Provide analysis summary"
        )

        response = llm.invoke(prompt)
        parsed_response = parse_json_response(response.content)

        query_results = []

        if isinstance(parsed_response, dict):
            if "query_results" in parsed_response:
                qr = parsed_response["query_results"]
                if isinstance(qr, list):
                    query_results.extend(qr)
                else:
                    query_results.append(qr)
            else:
                query_results.append(parsed_response)
        else:
            query_results.append({
                "Query": query if query else "General query",
                "Response": str(parsed_response),
                "Relevant_JE_IDs": "N/A"
            })

        if not query_results:
            query_results.append({
                "Query": query if query else "General query",
                "Response": "No specific results found in the analysis.",
                "Relevant_JE_IDs": "N/A"
            })

        return {
            "explanations": [],
            "query_results": query_results,
            "original_question": query if query else "General query"
        }

    except Exception as e:
        return {
            "explanations": [],
            "query_results": [{
                "Query": query if query else "General query",
                "Response": f"Error processing query: {str(e)}",
                "Relevant_JE_IDs": "N/A"
            }]
        }

def safe_answer_followup_questions(flagged_items, clean_items, ml_flagged, je_df, master_df, blackline_df, query=None, issue='Amount Exceeding Thresholds',
amount_threshold=500000, cutoff_date='2025-06-25', conversation_history=None, max_turns=4, overlap_turns=2, user_id="system_user"):
    """Enhanced with conversation history and similarity search"""
    try:
        from sap_chat_system_updated import SAPChatSystem
        
        print(f"\n{'='*80}")
        print(f"[SIMILARITY SEARCH] Starting for user: {user_id}")
        print(f"[SIMILARITY SEARCH] Query: {query}")
        
        # Initialize chat system for similarity search
        chat_system = SAPChatSystem(user_id)
        
        # Get similar conversations
        _, has_similar, examples = chat_system.get_response(query or "General query")
        
        print(f"[SIMILARITY SEARCH] Found similar conversations: {has_similar}")
        print(f"[SIMILARITY SEARCH] Number of examples: {len(examples)}")
        
        for i, ex in enumerate(examples, 1):
            print(f"\n[EXAMPLE {i}] Type: {ex['type']} | Rating: {ex['rating']}/5")
            print(f"[EXAMPLE {i}] Question: {ex['question'][:80]}...")
            print(f"[EXAMPLE {i}] Response: {ex['response'][:80]}...")
            if ex.get('feedback'):
                print(f"[EXAMPLE {i}] Feedback: {ex['feedback'][:80]}...")
            if ex.get('improved_response'):
                print(f"[EXAMPLE {i}] Improved: {ex['improved_response'][:80]}...")
        
        # Format conversation history
        history_text = ""
        if conversation_history:
            history_text = "\n\nPrevious Conversation Context:\n"
            for i, conv in enumerate(list(conversation_history)[-3:], 1):
                q = conv.get('question', '')
                a = conv.get('answer', '')
                history_text += f"{i}. Q: {q[:100]}...\n   A: {a[:100]}...\n"
            print(f"\n[CONVERSATION HISTORY] Loaded {len(list(conversation_history)[-3:])} previous exchanges")
        
        # Format similar examples
        examples_text = ""
        if examples:
            examples_text = "\n\nLearning from Similar Past Conversations:\n"
            for ex in examples:
                if ex['type'] == 'good':
                    examples_text += f"Good Example (Rating {ex['rating']}/5):\n  Q: {ex['question'][:100]}\n  A: {ex['response'][:100]}\n"
                elif ex['type'] == 'bad' and ex.get('improved_response'):
                    examples_text += f"Previous Issue (Rating {ex['rating']}/5):\n  Q: {ex['question'][:100]}\n  Original: {ex['response'][:80]}\n  Improved: {ex['improved_response'][:80]}\n"
            print(f"\n[PROMPT ENHANCEMENT] Added {len(examples)} examples to prompt")
        
        print(f"{'='*80}\n")
        
        llm = LLM_Chat()
        
        # Safe data conversion
        je_df_json = dataframe_for_json(je_df)
        blackline_df_json = dataframe_for_json(blackline_df)
        master_df_json = dataframe_for_json(master_df)
        
        safe_flagged = [json_dumps(item) for item in (flagged_items if flagged_items else [])]
        safe_clean = [json_dumps(item) for item in (clean_items if clean_items else [])]
        safe_ml = [json_dumps(item) for item in (ml_flagged if ml_flagged else [])]

        prompt_template = ChatPromptTemplate.from_template("""
        You are an accounts expert analyzing journal entries in SAP/BlackLine.
        {history_context}
        {examples_context}
        
        IMPORTANT: If the user asks follow-up questions like "explain in detail", "tell me more", or refers to previous context,
        check the conversation history above and provide detailed information about what was previously discussed.
        
        Answer the user query based on this data:
        - Flagged Items: {flagged_item}
        - Clean Items: {clean_item}
        - ML Flagged: {anomaly_item}
        - JE Details: {je_df}
        - Master: {master_df}
        - Reconciliation: {reconciliation_df}
        
        Current User Query: {user_query}
        
        Return JSON with this EXACT structure:
        {{
          "query_results": [{{
            "Response": "Your natural language answer here",
            "Contributing_Factors": "Comma-separated factors",
            "Relevant_JE_IDs": "Comma-separated JE IDs if applicable"
          }}]
        }}
        """)
        
        prompt = prompt_template.format(
            history_context=history_text,
            examples_context=examples_text,
            flagged_item=json.dumps(safe_flagged, default=str),
            clean_item=json.dumps(safe_clean, default=str),
            anomaly_item=json.dumps(safe_ml, default=str),
            je_df=json.dumps(je_df_json, default=str),
            master_df=json.dumps(master_df_json, default=str),
            reconciliation_df=json.dumps(blackline_df_json, default=str),
            user_query=query if query else "Provide analysis summary"
        )

        response = llm.invoke(prompt)
        parsed_response = parse_json_response(response.content)

        query_results = []

        if isinstance(parsed_response, dict):
            if "query_results" in parsed_response:
                qr = parsed_response["query_results"]
                if isinstance(qr, list):
                    query_results.extend(qr)
                else:
                    query_results.append(qr)
            else:
                query_results.append(parsed_response)
        else:
            query_results.append({
                "Query": query if query else "General query",
                "Response": str(parsed_response),
                "Relevant_JE_IDs": "N/A"
            })

        if not query_results:
            query_results.append({
                "Query": query if query else "General query",
                "Response": "No specific results found in the analysis.",
                "Relevant_JE_IDs": "N/A"
            })

        return {
            "explanations": [],
            "query_results": query_results,
            "original_question": query if query else "General query",
            "has_similar_history": has_similar
        }

    except Exception as e:
        print(f"Error in safe_answer_followup_questions: {e}")
        return {
            "explanations": [],
            "query_results": [{
                "Query": query if query else "General query",
                "Response": f"Error processing query: {str(e)}",
                "Relevant_JE_IDs": "N/A"
            }],
            "has_similar_history": False
        }

# Replace the original function
answer_followup_questions = safe_answer_followup_questions