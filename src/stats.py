# benchmark.py
import asyncio
import websockets
import time

async def mesurer_debit():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    print("Connexion à Binance pour test de charge (60 secondes)...")
    
    async with websockets.connect(url) as ws:
        msg_count = 0
        start_time = time.time()
        
        while time.time() - start_time < 60:
            await ws.recv()
            msg_count += 1
            
        duree = time.time() - start_time
        debit = msg_count / duree
        
        print("-" * 30)
        print(f"Total messages : {msg_count}")
        print(f"Débit d'ingestion : {debit:.2f} messages/seconde")
        print("-" * 30)

asyncio.run(mesurer_debit())