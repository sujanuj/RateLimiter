import os
import json
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from app.redis_client import get_redis
from app.algorithms.token_bucket import TokenBucket

EXEMPT_PREFIXES = ("/health", "/docs", "/openapi", "/redoc", "/dashboard", "/rate-limit")
_CAPACITY    = int(os.getenv("MIDDLEWARE_CAPACITY", 20))
_REFILL_RATE = float(os.getenv("MIDDLEWARE_REFILL_RATE", 5.0))

def _get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

class TokenBucketMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if any(path.startswith(p) for p in EXEMPT_PREFIXES):
            return await call_next(request)
        client_ip = _get_client_ip(request)
        identifier = f"middleware:{client_ip}"
        redis = await get_redis()
        bucket = TokenBucket(redis)
        result = await bucket.is_allowed(identifier, capacity=_CAPACITY, refill_rate=_REFILL_RATE)
        if not result.allowed:
            body = json.dumps({"error": "Too many requests", "limit": result.limit, "remaining": 0, "retry_after_seconds": result.reset_after_seconds})
            return Response(content=body, status_code=429, media_type="application/json", headers={"Retry-After": str(result.reset_after_seconds), "X-RateLimit-Limit": str(result.limit), "X-RateLimit-Remaining": "0"})
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"]     = str(result.limit)
        response.headers["X-RateLimit-Remaining"] = str(result.remaining)
        response.headers["X-RateLimit-Reset"]     = str(result.reset_after_seconds)
        return response
