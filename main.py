from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from supabase import create_client, Client
from dotenv import load_dotenv
from pydantic import BaseModel
import os
import datetime
from typing import Literal


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

@app.get("/health")
def health_check():
    return JSONResponse(content={"status": "ok"})

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


class OHLCQuery(BaseModel):
    symbol: str
    timeframe: str
    start_time: datetime.datetime
    end_time: datetime.datetime

@app.post("/query_ohlc")
def query_ohlc(payload: OHLCQuery):
    try:
        response = (
            supabase.table("ohlc_data")
            .select("*")
            .eq("symbol", payload.symbol)
            .eq("timeframe", payload.timeframe)
            .gte("time", payload.start_time.isoformat())
            .lte("time", payload.end_time.isoformat())
            .order("time", desc=False)
            .execute()
        )
        return {"data": response.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

