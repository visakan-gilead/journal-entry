import os
import pandas as pd
from fastapi import UploadFile

def create_folder(path: str):
    """
    Creates a folder if it doesn't exist.
    """
    os.makedirs(path, exist_ok=True)

def save_upload_file(upload_file: UploadFile, destination_folder: str) -> str:
    """
    Saves an uploaded file to the specified folder and returns the file path.
    """
    create_folder(destination_folder)
    file_path = os.path.join(destination_folder, upload_file.filename)
    with open(file_path, "wb") as f:
        f.write(upload_file.file.read())
    return file_path

def read_excel(file_path: str) -> pd.DataFrame:
    """
    Reads an Excel file and returns a pandas DataFrame.
    """
    return pd.read_excel(file_path)
