"""
Local models through Ollama (free, offline, no quota). No SDK needed.

    LLM_PROVIDER=ollama
    OLLAMA_MODEL=qwen2.5-coder:14b      # optional; any model you have pulled
    OLLAMA_HOST=http://localhost:11434  # optional
    OLLAMA_NUM_CTX=16384                # optional; context window in tokens

Setup:  brew install ollama  (or the app from ollama.com), then  ollama pull qwen2.5-coder:14b

Ollama's default context window is only a few thousand tokens and longer prompts are
truncated SILENTLY, so num_ctx is always sent: the migration prompt (trap list + legacy
code + contract + previous attempt + diff) needs well over the default.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

from .base import LLM, register

THINKING_FAMILIES = ("qwen3", "deepseek-r1", "gpt-oss")   # have a "thinking" mode to switch off


class OllamaLLM(LLM):
    provider = "ollama"

    def __init__(self) -> None:
        self.model = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:14b")
        self.host = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
        self.num_ctx = int(os.getenv("OLLAMA_NUM_CTX", "16384"))

    def _call(self, system: str, prompt: str, temperature: float, json_mode: bool) -> str:
        body = {
            "model": self.model,
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": prompt}],
            "stream": False,
            "options": {"temperature": temperature, "num_ctx": self.num_ctx},
            **({"format": "json"} if json_mode else {}),
            **({"think": False} if self.model.startswith(THINKING_FAMILIES) else {}),
        }
        req = urllib.request.Request(f"{self.host}/api/chat", data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        delay = 5.0
        for attempt in range(1, 4):
            try:
                with urllib.request.urlopen(req, timeout=900) as r:   # local generation can be slow
                    data = json.loads(r.read())
                if data.get("done_reason") == "length":
                    print(f"  ollama: reply hit the length limit (num_ctx {self.num_ctx}); "
                          f"raise OLLAMA_NUM_CTX if replies come back cut off")
                return data["message"]["content"]
            except urllib.error.HTTPError as e:
                msg = e.read().decode()[:300]
                if e.code == 404:
                    raise RuntimeError(f"Ollama has no model '{self.model}': run  ollama pull {self.model}") from e
                if e.code >= 500 and attempt < 3:
                    print(f"  ollama {e.code}, retry {attempt} in {delay:.0f}s")
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise RuntimeError(f"Ollama error {e.code}: {msg}") from e
            except (urllib.error.URLError, ConnectionError) as e:
                raise RuntimeError(f"Ollama is not reachable at {self.host} ({e}); start it with  "
                                   f"ollama serve  or open the Ollama app") from e
        raise RuntimeError("Ollama: retries exhausted")


register("ollama", OllamaLLM)
