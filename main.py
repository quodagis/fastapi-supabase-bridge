from fastapi import FastAPI, HTTPException
from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

@app.get("/h4_tpds")
def get_h4_tpds():
    try:
        data = (
            supabase.table("tpd_data")
            .select("*")
            .eq("timeframe", "4h")
            .execute()
        )
        return data.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
