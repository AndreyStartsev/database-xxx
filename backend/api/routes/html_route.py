import os
from fastapi import APIRouter
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel

router = APIRouter()


@router.get('/', description="Returns the index page.")
async def get_index():
    if os.path.exists("./backend/static/index.html"):
        return FileResponse("./backend/static/index.html")
    else:
        return HTMLResponse("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Anonymization API</title>
            <img src="https://www.oreilly.com/api/v2/epubs/9781449363062/files/images/anhd_0102.png" width="600">
            <h2>API docs: <a href="/docs">/docs</a></h2>
            <h2>API redoc: <a href="/redoc">/redoc</a></h2>
        </head>
        <body>

        </body>
        </html>
        """)
