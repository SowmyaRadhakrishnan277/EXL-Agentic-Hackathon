"""
Hosted models through OpenRouter (OpenAI-compatible REST API, no SDK needed).

    LLM_PROVIDER=openrouter        # use it for everything, or
    LLM_ESCALATE=openrouter        # keep the main (local) model and send only the jobs it fails
    OPENROUTER_API_KEY=...         # https://openrouter.ai/keys
    OPENROUTER_MODEL=a:free,b:free # optional; tried in order, the next one when a model is unavailable
    OPENROUTER_DAILY_LIMIT=45      # optional; requests this project may send per UTC day

Free models are limited to 20 requests a minute and 50 a day (1,000 a day once the account has
bought $10 of credit). The daily budget is counted here, in runs/llm_cache/_usage_openrouter.json,
and LLMBudgetError is raised BEFORE the limit is hit, so a run stops cleanly and the remaining
jobs are left for review instead of failing half way. Cached replies cost nothing.

Nothing that identifies the user or the project is sent: no Referer or title headers.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from .base import CACHE, LLM, LLMBudgetError, register

URL = "https://openrouter.ai/api/v1/chat/completions"
# Order chosen from the model cards (coding models with JSON mode first); not benchmarked.
DEFAULT_MODELS = ("particle-ai/particle-1:free,baidu/ernie-5.5-flash-preview:free,"
                  "nvidia/nemotron-3-ultra-550b-a55b:free")
USAGE = CACHE / "_usage_openrouter.json"


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def used_today() -> int:
    try:
        return int(json.loads(USAGE.read_text()).get(_today(), 0))
    except (OSError, ValueError):
        return 0


def _count_request() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    USAGE.write_text(json.dumps({_today(): used_today() + 1}))


class OpenRouterLLM(LLM):
    provider = "openrouter"

    def __init__(self) -> None:
        self.models = [m.strip() for m in os.getenv("OPENROUTER_MODEL", DEFAULT_MODELS).split(",") if m.strip()]
        self.model = self.models[0]
        self.key = os.getenv("OPENROUTER_API_KEY", "")
        self.daily_limit = int(os.getenv("OPENROUTER_DAILY_LIMIT", "45"))
        self._json_mode_ok = True

    @property
    def available(self) -> bool:
        return bool(self.key)

    @property
    def budget_left(self) -> int:
        return max(0, self.daily_limit - used_today())

    def _next_model(self, why: str) -> bool:
        i = self.models.index(self.model)
        if i + 1 >= len(self.models):
            return False
        print(f"  openrouter: {self.model} {why}; switching to {self.models[i + 1]}")
        self.model = self.models[i + 1]
        self._json_mode_ok = True
        return True

    def _post(self, body: dict) -> dict:
        if self.budget_left <= 0:
            raise LLMBudgetError(f"OpenRouter daily budget used ({self.daily_limit} requests, "
                                 f"OPENROUTER_DAILY_LIMIT); resets at 00:00 UTC")
        _count_request()
        req = urllib.request.Request(URL, data=json.dumps(body).encode(), headers={
            "Content-Type": "application/json", "Authorization": f"Bearer {self.key}"})
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            try:
                err = json.loads(e.read()).get("error", {})
            except ValueError:
                err = {}
            return {"error": {"code": e.code, "message": str(err.get("message", ""))[:300],
                              "metadata": err.get("metadata", {})}}

    def _call(self, system: str, prompt: str, temperature: float, json_mode: bool) -> str:
        if not self.key:
            raise RuntimeError("OPENROUTER_API_KEY is not set (see .env.example)")
        delay, tries = 5.0, 0
        while True:
            tries += 1
            body = {"model": self.model, "temperature": temperature,
                    "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                    **({"response_format": {"type": "json_object"}} if json_mode and self._json_mode_ok else {})}
            try:
                data = self._post(body)
            except (TimeoutError, urllib.error.URLError, ConnectionError) as e:
                data = {"error": {"code": 503, "message": f"{type(e).__name__}: {e}"}}
            err = data.get("error")
            if not err:
                text = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
                text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()   # reasoning models
                if text:
                    return text
                err = {"code": 502, "message": "empty reply"}
            code, msg = int(err.get("code") or 500), str(err.get("message", ""))
            if code == 401:
                raise RuntimeError(f"OpenRouter rejected the API key: {msg}")
            if code == 429 and re.search(r"per[- ]day|daily", msg, re.I):
                raise LLMBudgetError(f"OpenRouter daily free limit reached: {msg}")
            if code == 400 and json_mode and self._json_mode_ok and "response_format" in json.dumps(body):
                self._json_mode_ok = False         # this model has no JSON mode: ask in plain text
                continue
            if code in (429, 500, 502, 503, 504) and tries < 4:
                print(f"  openrouter {code}, retry {tries} in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            if code in (400, 402, 404, 408, 429, 500, 502, 503, 504) and self._next_model(f"gave {code}"):
                delay, tries = 5.0, 0
                continue
            raise RuntimeError(f"OpenRouter error {code} from {self.model}: {msg}")


register("openrouter", OpenRouterLLM)
