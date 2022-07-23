import aiohttp
import asyncio
import os

async def get_entities(base_uri, text):
    uri = f'{base_uri}/extract'
    async with aiohttp.ClientSession() as session:
        async with session.post(uri, json={'text': text}) as resp:
            print(resp.status)
            print(await resp.json())

uri = os.getenv('NLP_URI', 'http://')
# # print(uri)
asyncio.run(get_entities(uri, test_text))