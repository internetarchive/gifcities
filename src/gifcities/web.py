import contextlib
from enum import StrEnum
from typing import Any, AsyncIterator, TypedDict

import numpy
import open_clip
import pydantic
import torch
from elasticsearch import Elasticsearch
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response
from starlette.routing import Route, Mount
from starlette.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles

from gifcities.config import settings

MAX_PAGE_SIZE = 50
DEFAULT_PAGE_SIZE = 20
EMBEDDING_MODEL = "ViT-L-14"
EMBEDDING_PRETRAIN = "laion2b_s32b_b82k"

tmpls = Jinja2Templates(directory='src/gifcities/templates')

es_client = Elasticsearch(
        settings.ELASTICSEARCH_URL,
        ca_certs=settings.ELASTICSEARCH_CERT,
        basic_auth=(settings.ELASTICSEARCH_USER, settings.ELASTICSEARCH_PASSWORD),
        )

class EmbeddedQuery(pydantic.BaseModel):
    query: str
    embedding: list[float]

class QueryEmbedder:
    def __init__(self, model_name=EMBEDDING_MODEL, pretrain=EMBEDDING_PRETRAIN):
        self.model_name = model_name
        self.pretrain = pretrain
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, _, preprocess = open_clip.create_model_and_transforms(
                self.model_name, self.pretrain, device=self.device)
        self.tokenizer = open_clip.get_tokenizer(self.model_name)

    def calculate_embedding(self, text) -> EmbeddedQuery:
        return self.calculate(text, self.model, self.tokenizer, self.device)

    def l2_normalize(self, vectors: list[float]) -> list[float]:
        vectors_np = numpy.array(vectors)
        norms = numpy.linalg.norm(vectors_np, ord=2, axis=1, keepdims=True)
        normalized_vectors = vectors_np / norms
        return normalized_vectors.tolist()

    # TODO clip is not good about types; model/tokenizer can be various classes
    # without a common hierarchy :(
    # TODO will i need to normalize?
    def calculate(self, text: str, model: Any, tokenizer: Any, device: str):
        with torch.no_grad():
            tokenized_text = tokenizer([text]).to(device)
            text_features = model.encode_text(tokenized_text)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
            lol = text_features.cpu().numpy().tolist()[0]
            return EmbeddedQuery(
                    query=text,
                    #embedding=self.l2_normalize(lol))
                    embedding=lol)


async def index(request: Request) -> Response:
    return tmpls.TemplateResponse(request, "index.html", {"page_size": DEFAULT_PAGE_SIZE})

async def about(request: Request) -> Response:
    return tmpls.TemplateResponse(request, "about.html")

async def favicon(request: Request) -> RedirectResponse:
    return RedirectResponse(status_code=301, url="/static/favicon.ico")

class GeocitiesPage(pydantic.BaseModel):
    url: str
    timestamp: str

class GifUse(pydantic.BaseModel):
    url: str
    timestamp: str
    path: str
    filename: str
    page: GeocitiesPage|None

class Gif(pydantic.BaseModel):
    checksum: str
    page_count: int
    uses: list[GifUse]


class SearchFlavor(StrEnum):
    LEXICAL = "lexical"
    SEMANTIC = "semantic"


async def search(request: Request) -> Response:
    q = request.query_params.get('q', 'geocities')
    o = request.query_params.get('offset', "0")
    ps = request.query_params.get('page_size', str(DEFAULT_PAGE_SIZE))
    flavor = request.query_params.get('flavor', SearchFlavor.LEXICAL)
    page_size = DEFAULT_PAGE_SIZE
    offset = 0
    try:
        page_size = int(ps)
    except ValueError:
        pass

    try:
        offset = int(o)
    except ValueError:
        pass

    if page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    # TODO debugging
    print(f"page size: {page_size}")
    print(f"q: {q}")
    print(f"flavor: {flavor}")
    print(f"offset: {offset}")

    if flavor == SearchFlavor.SEMANTIC:
        qe = request.state.query_embedder
        eq = qe.calculate_embedding(q)
        query = {
            "field": "vecs.vector",
            "query_vector": eq.embedding,
            "k": page_size,
            "num_candidates": 100, # candidates per shard; lower is faster/less accurate
        }
        # TODO how to paginate?
        resp = es_client.search(
                request_timeout=settings.ELASTICSEARCH_TIMEOUT,
                index=settings.ELASTICSEARCH_INDEX,
                from_=offset,
                size=page_size,
                sort='page_count:desc',
                # TODO use fields to trim down response size (ie do not need vecs)
                knn=query)
    elif flavor == SearchFlavor.LEXICAL:
        query = {
            "nested": {
            "path": "uses",
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["uses.filename^3", "uses.path"]
                    }
                }
            }
        }
        resp = es_client.search(
                request_timeout=settings.ELASTICSEARCH_TIMEOUT,
                index=settings.ELASTICSEARCH_INDEX,
                from_=offset,
                size=page_size,
                sort='page_count:desc',
                query=query)
    else:
        return HTMLResponse(content="unsupported search flavor", status_code=400)

    results: list[Gif] = []

    # TODO use async query to ES

    for h in resp['hits']['hits']:
        doc = h['_source']
        uses = []
        for u in doc['uses']:
            page = None
            if u['page'] != None:
                page = GeocitiesPage(url=u['page']['url'], timestamp=u['page']['timestamp'])

            uses.append(GifUse(url=u['url'],
                               timestamp=u['timestamp'],
                               path=u['path'],
                               filename=u['filename'],
                               page=page))

        results.append(Gif(
            checksum=doc['checksum'],
            page_count=doc['page_count'],
            uses=uses))

    # TODO debugging
    del resp['hits']['hits']
    print(resp)

    ctx = {
        "q": q,
        "results": results,
        "current_page": int(offset / page_size) + 1,
        "settings": settings,
        "offset": offset,
        "total_pages": int(resp['hits']['total']['value'] / page_size) + 1,
        "page_size": page_size,
        "flavor": flavor,
    }

    return tmpls.TemplateResponse(request, "results.html", ctx)

class State(TypedDict):
    query_embedder: QueryEmbedder

@contextlib.asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[State]:
    yield {'query_embedder': QueryEmbedder()}

app = Starlette(debug=settings.DEBUG, lifespan=lifespan, routes=[
    Route('/', index),
    Route('/about', about),
    Route('/favicon.ico', favicon),
    Route('/search', search),
    Mount('/static', StaticFiles(directory='src/gifcities/static'), name='static'),
])

#if settings.SENTRY_DSN:
#    logger.info("Sentry integration enabled")
#    sentry_sdk.init(
#        dsn=settings.SENTRY_DSN,
#        environment=settings.SCHOLAR_ENV,
#        max_breadcrumbs=10,
#        release=GIT_REVISION,
#    )
#    app.add_middleware(SentryAsgiMiddleware)
