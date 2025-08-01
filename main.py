from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import JSONResponse
from supabase import create_client, Client
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Literal, List, Optional
import os
import datetime

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
    symbol: Literal["ESM2025", "NQM2025", "YMM2025"]
    timeframe: Literal["60", "240", "360", "1D", "1W"]
    start_time: datetime.datetime
    end_time: datetime.datetime
    limit: Optional[int] = 1000
    offset: Optional[int] = 0

@app.post("/query_ohlc")
def query_ohlc(payload: OHLCQuery):
    try:
        limit = payload.limit or 1000
        offset = payload.offset or 0
        end_index = offset + limit - 1  # Supabase .range is inclusive

        print(f"Querying: symbol={payload.symbol}, timeframe={payload.timeframe}, time={payload.start_time}–{payload.end_time}, range={offset}–{end_index}")

        response = (
            supabase.table("ohlc_data")
            .select("*")
            .eq("symbol", str(payload.symbol))
            .eq("timeframe", str(payload.timeframe))
            .gte("time", payload.start_time.isoformat())
            .lte("time", payload.end_time.isoformat())
            .order("time", desc=False)
            .range(offset, end_index)
            .execute()
        )
        return {"data": response.data}
        print(f"Response: {response}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/insert_tpds")
def insert_tpds(records: List[dict] = Body(...)):
    try:
        supabase.table("tpd_data").insert(records).execute()
        return {"status": "success", "inserted": len(records)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tpd_exists")
def tpd_exists(tpd_time: str, timeframe: str):
    try:
        result = supabase.table("tpd_data") \
            .select("tpd_time", "timeframe") \
            .eq("tpd_time", tpd_time) \
            .eq("timeframe", timeframe) \
            .execute()

        return {"exists": len(result.data) > 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
