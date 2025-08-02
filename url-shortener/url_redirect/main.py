from typing import Union

from fastapi import FastAPI
from fastapi.responses import RedirectResponse, JSONResponse
import redis

app = FastAPI()

r = redis.Redis(host="localhost", port=6379, db=0)


@app.get("/")
async def home():
    """Redirect to the long URL based on the short URL."""
    return JSONResponse(
        content={"message": "Please provide a short URL to redirect."},
        status_code=400,
    )


@app.get("/short_url/{short_url}", response_class=RedirectResponse)
async def redirect(short_url: str):
    """Redirect to the long URL based on the short URL."""
    print(r)
    long_url: Union[bytes, None] = r.get(short_url)

    if long_url:
        long_url_decoded = long_url.decode("utf-8")
        print(f"Redirecting to: {long_url_decoded}")
        if "https://" in long_url_decoded:
            return RedirectResponse(url=long_url_decoded)
        else:
            return RedirectResponse(url=f"https://{long_url.decode('utf-8')}")
    else:
        return RedirectResponse(url="https://example.com/404")
