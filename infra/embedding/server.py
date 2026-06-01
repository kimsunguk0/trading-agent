"""bge-m3 embedding server — OpenAI-compatible /v1/embeddings endpoint."""

from __future__ import annotations

import os
import time
from typing import Any

import torch
from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

MODEL_PATH = os.getenv("MODEL_PATH", "/models/bge-m3")
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

app = FastAPI(title="bge-m3 embedding server")
_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(MODEL_PATH, device=DEVICE)
    return _model


@app.on_event("startup")
async def startup() -> None:
    get_model()
    print(f"bge-m3 loaded on {DEVICE}", flush=True)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "device": DEVICE, "model": MODEL_PATH}


class EmbedRequest(BaseModel):
    input: list[str] | str
    model: str = "bge-m3"


class EmbedData(BaseModel):
    object: str = "embedding"
    index: int
    embedding: list[float]


class EmbedUsage(BaseModel):
    prompt_tokens: int
    total_tokens: int


class EmbedResponse(BaseModel):
    object: str = "list"
    data: list[EmbedData]
    model: str
    usage: EmbedUsage


@app.post("/v1/embeddings", response_model=EmbedResponse)
def embed(req: EmbedRequest) -> Any:
    texts = [req.input] if isinstance(req.input, str) else req.input
    model = get_model()
    vecs = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    data = [EmbedData(index=i, embedding=v.tolist()) for i, v in enumerate(vecs)]
    total_tokens = sum(len(t.split()) for t in texts)
    return EmbedResponse(
        data=data,
        model=req.model,
        usage=EmbedUsage(prompt_tokens=total_tokens, total_tokens=total_tokens),
    )
