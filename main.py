from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from supabase import create_client, Client
from dotenv import load_dotenv
from pydantic import BaseModel
import asyncpg
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

    symbol: Literal["ES", "NQ", "YM"]
    timeframe: Literal["Weekly", "Daily", "H4", "H1"]
    start_time: datetime.datetime
    end_time: datetime.datetime

@app.post("/query_ohlc")
async def query_ohlc(payload: OHLCQuery):
    try:
        conn = await asyncpg.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST"),
            port=5432,
            ssl='require'
        )

        rows = await conn.fetch(
            """
            SELECT symbol, timeframe, timestamp, open, high, low, close
            FROM ohlc_data
            WHERE symbol = $1
              AND timeframe = $2
              AND timestamp >= $3
              AND timestamp <= $4
            ORDER BY timestamp ASC
            """,
            payload.symbol, payload.timeframe,
            payload.start_time, payload.end_time
        )

        await conn.close()

        return {"data": [dict(row) for row in rows]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
