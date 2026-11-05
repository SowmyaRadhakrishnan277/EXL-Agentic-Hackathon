"""
Google Gemini via the REST API (no SDK needed).

    LLM_PROVIDER=gemini
    GEMINI_API_KEY=...            # free key from https://aistudio.google.com/apikey
    GEMINI_MODEL=gemini-2.5-flash # optional

Free-tier keys are rate-limited, so 429/5xx replies are retried with exponential
backoff before giving up.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

from .base import LLM, register

URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"


class GeminiLLM(LLM):
    provider = "gemini"

    def __init__(self) -> None:
        self.model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        self.key = os.getenv("GEMINI_API_KEY", "")

    def _call(self, system: str, prompt: str, temperature: float, json_mode: bool) -> str:
        if not self.key:
            raise RuntimeError("GEMINI_API_KEY is not set (see .env.example)")
        body = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": temperature,
                                 **({"responseMimeType": "application/json"} if json_mode else {})},
        }
        req = urllib.request.Request(
            URL.format(model=self.model), data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "x-goog-api-key": self.key})
        delay = 5.0
        for attempt in range(1, 6):
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    data = json.loads(r.read())
                parts = data["candidates"][0]["content"]["parts"]
                return "".join(p.get("text", "") for p in parts)
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503, 504) and attempt < 5:
                    print(f"  gemini {e.code}, retry {attempt} in {delay:.0f}s")
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise RuntimeError(f"Gemini error {e.code}: {e.read().decode()[:300]}") from e
        raise RuntimeError("Gemini: retries exhausted")


register("gemini", GeminiLLM)
