"""
Chat and Search API routes.

Provides:
- Quick search endpoint (no LLM, fast)
- Chat endpoint (LLM-powered)
- SSE streaming for chat responses
"""
import os
import json
from typing import Optional
from fastapi import APIRouter, Query, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from web.ratelimit import limiter  # single app-wide limiter instance
from web.services.search_service import get_search_service
from web.services.chat_service import get_chat_service
from web.routes.auth import require_admin
from core.observability import get_logger
from core.config import config

logger = get_logger(__name__)

router = APIRouter(tags=["chat"])

# Dev mode - skip auth for LOCAL development only. Hard-disabled in production
# (DASHBOARD_URL is https) so a stray DEV_MODE=1 in a prod .env cannot open up
# the LLM/search endpoints to the world.
_IS_PROD = os.getenv("DASHBOARD_URL", "").startswith("https://")
DEV_MODE = os.getenv("DEV_MODE", "").lower() in ("1", "true", "yes") and not _IS_PROD
if os.getenv("DEV_MODE", "").lower() in ("1", "true", "yes") and _IS_PROD:
    logger.critical("DEV_MODE is set but ignored — refusing to bypass auth on a production (https) deployment")


async def require_admin_or_dev(request: Request) -> dict:
    """Allow access in dev mode, otherwise require admin."""
    if DEV_MODE:
        return {"user_id": 0, "username": "dev"}
    return await require_admin(request)


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST/RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class ChatRequest(BaseModel):
    """Chat message request."""
    message: str = Field(..., min_length=1, max_length=2000)
    conversation_id: Optional[str] = None
    context: Optional[dict] = Field(default_factory=dict)


class ChatResponse(BaseModel):
    """Chat response."""
    conversation_id: str
    content: str
    tokens_used: Optional[int] = None
    error: Optional[bool] = None


# ═══════════════════════════════════════════════════════════════════════════════
# CHAT ENDPOINTS (LLM-powered)
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/chat", response_model=ChatResponse)
@limiter.limit("10/minute")
async def chat(
    request: Request,
    body: ChatRequest,
):
    """
    Send a message to the AI assistant.

    The assistant can answer questions about sales data, find customers/orders,
    and provide business insights. Responds in the same language as the query.
    """
    service = get_chat_service()

    if not service.is_available:
        raise HTTPException(
            status_code=503,
            detail="Chat service is not available. Please configure ANTHROPIC_API_KEY."
        )

    # Get or create conversation
    conv_id = body.conversation_id
    if not conv_id:
        conv_id = service.create_conversation(body.context)

    # Get response
    result = await service.chat(conv_id, body.message)

    return ChatResponse(
        conversation_id=result["conversation_id"],
        content=result["content"],
        tokens_used=result.get("tokens_used"),
        error=result.get("error")
    )


@router.get("/chat/stream")
@limiter.limit("10/minute")
async def chat_stream(
    request: Request,
    message: str = Query(..., min_length=1, max_length=2000),
    conversation_id: Optional[str] = Query(None),
):
    """
    Stream chat response using Server-Sent Events (SSE).

    Events:
    - chunk: Text chunk from the assistant
    - tool_call: Tool being called
    - tool_result: Result from tool execution
    - end: Conversation finished (includes tokens_used)
    - error: An error occurred
    """
    service = get_chat_service()

    if not service.is_available:
        async def error_generator():
            yield {
                "event": "error",
                "data": json.dumps({"error": "Chat service not available"})
            }
        return EventSourceResponse(error_generator())

    # Get or create conversation
    conv_id = conversation_id
    if not conv_id:
        conv_id = service.create_conversation()

    async def event_generator():
        try:
            async for event in service.chat_stream(conv_id, message):
                event_type = event.get("type", "chunk")

                if event_type == "chunk":
                    yield {
                        "event": "chunk",
                        "data": json.dumps({"text": event.get("text", "")})
                    }
                elif event_type == "tool_call":
                    yield {
                        "event": "tool_call",
                        "data": json.dumps({
                            "tool": event.get("tool"),
                            "input": event.get("input")
                        })
                    }
                elif event_type == "tool_result":
                    yield {
                        "event": "tool_result",
                        "data": json.dumps({
                            "tool": event.get("tool"),
                            "result": event.get("result")
                        })
                    }
                elif event_type == "end":
                    yield {
                        "event": "end",
                        "data": json.dumps({
                            "conversation_id": conv_id,
                            "tokens_used": event.get("tokens_used")
                        })
                    }
                elif event_type == "error":
                    yield {
                        "event": "error",
                        "data": json.dumps({"error": event.get("error")})
                    }

        except Exception as e:
            logger.error(f"Chat stream error: {e}")
            yield {
                "event": "error",
                "data": json.dumps({"error": str(e)})
            }

    return EventSourceResponse(event_generator())


@router.get("/chat/status")
async def chat_status():
    """Get chat service status."""
    service = get_chat_service()
    return {
        "available": service.is_available,
        "active_conversations": len(service._conversations)
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SEARCH ENDPOINTS (No LLM, fast)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/search")
async def search(
    q: str = Query(..., min_length=2, description="Search query"),
    type: str = Query("all", description="Search type: buyers, orders, products, or all"),
    limit: int = Query(10, ge=1, le=50, description="Max results per type"),
):
    """
    Quick inline search across buyers, orders, and products.

    Uses Meilisearch for typo-tolerant, fast search (<10ms).
    No LLM involvement - direct database search.
    """
    service = get_search_service()
    results = await service.search(q, type, limit)
    return results


@router.get("/search/buyers")
async def search_buyers(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    city: Optional[str] = Query(None),
):
    """Search buyers by name, phone, or email."""
    service = get_search_service()
    return await service.search_buyers(q, limit, city)


@router.get("/search/orders")
async def search_orders(
    q: str = Query(..., min_length=1, description="Search query (order ID or buyer name)"),
    limit: int = Query(10, ge=1, le=50),
):
    """Search orders by ID or buyer name."""
    service = get_search_service()
    return await service.search_orders(q, limit)


@router.get("/search/products")
async def search_products(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    brand: Optional[str] = Query(None),
):
    """Search products by name, SKU, or brand."""
    service = get_search_service()
    return await service.search_products(q, limit, brand)


@router.get("/buyers/{buyer_id}")
async def get_buyer_details(
    buyer_id: int,
):
    """Get full buyer profile with order history."""
    service = get_search_service()
    result = await service.get_buyer_details(buyer_id)
    if not result:
        raise HTTPException(status_code=404, detail="Buyer not found")
    return result


@router.get("/orders/{order_id}")
async def get_order_details(
    order_id: int,
):
    """Get full order details with products."""
    service = get_search_service()
    result = await service.get_order_details(order_id)
    if not result:
        raise HTTPException(status_code=404, detail="Order not found")
    return result


@router.get("/products/{product_id}")
async def get_product_details(
    product_id: int,
):
    """Get product details with sales stats."""
    service = get_search_service()
    result = await service.get_product_details(product_id)
    if not result:
        raise HTTPException(status_code=404, detail="Product not found")
    return result
