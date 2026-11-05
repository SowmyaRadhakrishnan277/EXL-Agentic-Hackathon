"""
Pluggable LLM interface. Agents only ever call `get_llm().complete(...)` or
`get_llm().complete_json(...)`; the provider is picked with the LLM_PROVIDER env var.

Every reply is cached under runs/llm_cache/ keyed by (provider, model, prompt), so
re-running a stage costs nothing, stays within free-tier quotas, and gives the
same answer every time (useful for a reproducible demo).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from abc import ABC, abstractmethod
from pathlib import Path

CACHE = Path(__file__).resolve().parents[2] / "runs" / "llm_cache"


class LLM(ABC):
    provider: str = "base"
    model: str = "none"

    @abstractmethod
    def _call(self, system: str, prompt: str, temperature: float, json_mode: bool) -> str:
        """Provider-specific call returning the model's text reply."""

    def complete(self, system: str, prompt: str, temperature: float = 0.0,
                 json_mode: bool = False) -> str:
        key = hashlib.sha256(json.dumps([self.provider, self.model, system, prompt,
                                         temperature, json_mode]).encode()).hexdigest()[:24]
        path = CACHE / f"{key}.json"
        if path.exists():
            return json.loads(path.read_text())["reply"]
        reply = self._call(system, prompt, temperature, json_mode)
        CACHE.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"provider": self.provider, "model": self.model,
                                    "system": system, "prompt": prompt, "reply": reply}, indent=1))
        return reply

    def complete_json(self, system: str, prompt: str, temperature: float = 0.0) -> dict:
        text = self.complete(system, prompt, temperature, json_mode=True)
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip())
        return json.loads(text)

    @property
    def available(self) -> bool:
        return True


class EchoLLM(LLM):
    """Offline placeholder: lets the pipeline run with no AI (parser-only results)."""
    provider = "echo"
    model = "none"

    def _call(self, system, prompt, temperature, json_mode) -> str:
        return "{}" if json_mode else ""

    @property
    def available(self) -> bool:
        return False


_REGISTRY: dict[str, type[LLM]] = {"echo": EchoLLM}


def register(name: str, cls: type[LLM]) -> None:
    _REGISTRY[name] = cls


def get_llm(name: str | None = None) -> LLM:
    from . import gemini  # noqa: F401  (registers "gemini")
    name = name or os.getenv("LLM_PROVIDER", "echo")
    if name not in _REGISTRY:
        raise ValueError(f"Unknown LLM provider '{name}'. Registered: {sorted(_REGISTRY)}")
    return _REGISTRY[name]()
