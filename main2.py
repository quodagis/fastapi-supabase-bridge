from fastapi import FastAPI, HTTPException
from fastapi import Body
from fastapi.responses import JSONResponse
from supabase import create_client, Client
from dotenv import load_dotenv
from pydantic import BaseModel
import asyncpg
import os
import datetime
from typing import Literal
from typing import List


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
    limit: Optional[int] = 5000
    offset: Optional[int] = 0

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
            SELECT symbol, timeframe, time AS timestamp, open, high, low, close
            FROM ohlc_data
            WHERE symbol = $1
              AND timeframe = $2
              AND time >= $3
              AND time <= $4
            ORDER BY time ASC
            LIMIT $5 OFFSET $6
            """,
            payload.symbol,
            payload.timeframe,
            payload.start_time,
            payload.end_time,
            payload.limit,
            payload.offset
        )

        await conn.close()

        return {"data": [dict(row) for row in rows]}

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


