import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabricksLLM:
    def __init__(self):
        self.api_key = os.getenv("DATABRICKS_API_KEY")
        self.base_url = os.getenv("DATABRICKS_BASE_URL")
        self.model = os.getenv("DATABRICKS_MODEL", "databricks-claude-sonnet-4-5")
        
        if not self.api_key or not self.base_url:
            raise ValueError("DATABRICKS_API_KEY and DATABRICKS_BASE_URL must be set in environment variables")
    
    def invoke(self, prompt):
        """Invoke Databricks LLM with a prompt"""
        print(f"LLM invoke called with prompt length: {len(prompt)}")
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": 4000,
            "temperature": 0.1
        }
        
        try:
            # Construct the correct URL - base_url already includes the path
            url = f"{self.base_url.rstrip('/')}/{self.model}/invocations"
            print(f"Making request to: {url}")
            
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=60
            )
            
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"LLM response received successfully")
                
                # Extract content from Databricks response format
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    print(f"LLM content length: {len(content)}")
                    return DatabricksResponse(content)
                else:
                    print("No choices in LLM response")
                    return DatabricksResponse("No response generated")
            else:
                print(f"Error response: {response.text}")
                return DatabricksResponse(f"Error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"Exception in LLM invoke: {str(e)}")
            return DatabricksResponse(f"Error calling Databricks LLM: {str(e)}")

class DatabricksResponse:
    def __init__(self, content):
        self.content = content

def LLM_Chat():
    """Factory function to create Databricks LLM instance"""
    try:
        print("Initializing Databricks LLM...")
        llm = DatabricksLLM()
        print("Databricks LLM initialized successfully")
        return llm
    except Exception as e:
        print(f"Error initializing Databricks LLM: {e}")
        return None