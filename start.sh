#!/bin/bash
uvicorn backend.app:app --host 0.0.0.0 --port 8000 &
streamlit run Login.py --server.port 8501 --server.address 0.0.0.0