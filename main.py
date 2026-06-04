import os
import asyncio
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
import database

app = FastAPI(title="PriceBot Pro", version="5.0")

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "pricebot_verify_2026")
META_TOKEN = os.getenv("WHATSAPP_TOKEN", os.getenv("WHATSAPP_ACCESS_TOKEN", ""))
PHONE_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "")

http_client = httpx.AsyncClient(timeout=20.0)
queue = asyncio.Queue()

async def send_whatsapp_message(to_number: str, text: str):
    """إرسال رسائل ميتا بدون تجميد السيرفر"""
    if not META_TOKEN or not PHONE_ID:
        print("Error: Meta Token or Phone ID is missing in .env")
        return
        
    url = f"https://graph.facebook.com/v20.0/{PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {META_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "text",
        "text": {"body": text}
    }
    try:
        await http_client.post(url, json=payload, headers=headers)
    except Exception as e:
        print(f"Send Error: {e}")

async def process_message(payload: dict):
    """المعالجة الخلفية للرسالة"""
    for entry in payload.get("entry", []):
        for change in entry.get("changes", []):
            value = change.get("value", {})
            if "statuses" in value:
                continue
                
            for msg in value.get("messages", []):
                phone = msg.get("from")
                if not phone:
                    continue
                
                # هنا سنجلب ملف matcher لمعالجة النص والصور
                # وسنمرر حالة الزبون المحفوظة في قاعدة البيانات
                user_state = database.get_user_state(phone)
                
                if msg.get("type") == "text":
                    text = msg.get("text", {}).get("body", "")
                    await send_whatsapp_message(phone, f"لقد استلمت رسالتك: {text}")
                    # database.update_user_state(phone, {"last_msg": text})

async def webhook_worker():
    while True:
        payload = await queue.get()
        try:
            await process_message(payload)
        except Exception as e:
            print(f"Worker Error: {e}")
        finally:
            queue.task_done()

@app.on_event("startup")
async def startup_event():
    for _ in range(5): # تشغيل 5 عمال لمعالجة الرسائل بلمح البصر
        asyncio.create_task(webhook_worker())

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    """استلام الرسالة والرد على ميتا فوراً لتجنب التكرار والحظر"""
    payload = await request.json()
    await queue.put(payload)
    return JSONResponse({"ok": True, "queued": True})

@app.get("/webhook/whatsapp")
async def verify_webhook(request: Request):
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == VERIFY_TOKEN:
        return PlainTextResponse(params.get("hub.challenge", ""))
    return PlainTextResponse("Forbidden", status_code=403)
