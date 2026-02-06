"""
LLM client for chat assistant using Anthropic Claude.

Provides streaming responses and function calling capabilities.
Supports Ukrainian, Russian, and English responses.
"""
import asyncio
from typing import Optional, List, Dict, Any, AsyncGenerator
from dataclasses import dataclass

import anthropic
from anthropic import AsyncAnthropic

from core.config import config
from core.observability import get_logger

logger = get_logger(__name__)


@dataclass
class ToolCall:
    """Represents a tool call from the LLM."""
    id: str
    name: str
    input: Dict[str, Any]


@dataclass
class ChatMessage:
    """Chat message."""
    role: str  # "user" or "assistant"
    content: str
    tool_calls: Optional[List[ToolCall]] = None
    tool_results: Optional[List[Dict[str, Any]]] = None


class LLMClient:
    """Async client for Claude LLM."""

    SYSTEM_PROMPT = """You are a helpful sales analytics assistant for KoreanStory, a Korean cosmetics retailer.
You help users analyze sales data, find customers and orders, and answer questions about business performance.

Important guidelines:
- Respond in the same language the user uses (Ukrainian, Russian, or English)
- Use ₴ (UAH) for currency formatting
- Format dates as DD.MM.YYYY
- Be concise and data-focused
- When presenting search results, format them clearly
- For numbers, use spaces as thousand separators (e.g., 850 000)

Available data sources:
- Sales by source (Instagram, Telegram, Shopify)
- Top products by revenue and quantity
- Customer insights (new vs returning, AOV)
- Order and buyer search

When using tools, prefer to answer with specific data rather than generic responses."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        self.api_key = api_key
        self.model = model
        self._client: Optional[AsyncAnthropic] = None

    @property
    def client(self) -> AsyncAnthropic:
        """Lazy-initialize Anthropic client."""
        if self._client is None:
            self._client = AsyncAnthropic(api_key=self.api_key)
        return self._client

    @property
    def is_available(self) -> bool:
        """Check if LLM is configured."""
        return bool(self.api_key)

    async def chat(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict[str, Any]]] = None,
        max_tokens: int = 1000
    ) -> Dict[str, Any]:
        """
        Send a chat message and get a response (non-streaming).

        Args:
            messages: List of message dicts with role and content
            tools: Optional list of tool definitions
            max_tokens: Max response tokens

        Returns:
            Response dict with content, tool_calls, etc.
        """
        if not self.is_available:
            return {
                "content": "Chat assistant is not configured. Please set ANTHROPIC_API_KEY.",
                "error": True
            }

        try:
            kwargs = {
                "model": self.model,
                "max_tokens": max_tokens,
                "system": self.SYSTEM_PROMPT,
                "messages": messages,
            }

            if tools:
                kwargs["tools"] = tools

            response = await self.client.messages.create(**kwargs)

            # Parse response
            result = {
                "id": response.id,
                "content": "",
                "tool_calls": [],
                "stop_reason": response.stop_reason,
                "usage": {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens
                }
            }

            for block in response.content:
                if block.type == "text":
                    result["content"] += block.text
                elif block.type == "tool_use":
                    result["tool_calls"].append({
                        "id": block.id,
                        "name": block.name,
                        "input": block.input
                    })

            return result

        except anthropic.APIError as e:
            logger.error(f"Anthropic API error: {e}")
            return {
                "content": f"API error: {e.message}",
                "error": True
            }
        except Exception as e:
            logger.error(f"LLM chat error: {e}")
            return {
                "content": f"Error: {str(e)}",
                "error": True
            }

    async def chat_stream(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict[str, Any]]] = None,
        max_tokens: int = 1000
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Send a chat message and stream the response.

        Args:
            messages: List of message dicts with role and content
            tools: Optional list of tool definitions
            max_tokens: Max response tokens

        Yields:
            Event dicts: {"type": "text", "text": "..."} or {"type": "tool_use", ...}
        """
        if not self.is_available:
            yield {
                "type": "error",
                "error": "Chat assistant is not configured. Please set ANTHROPIC_API_KEY."
            }
            return

        try:
            kwargs = {
                "model": self.model,
                "max_tokens": max_tokens,
                "system": self.SYSTEM_PROMPT,
                "messages": messages,
            }

            if tools:
                kwargs["tools"] = tools

            async with self.client.messages.stream(**kwargs) as stream:
                current_tool = None

                async for event in stream:
                    if event.type == "content_block_start":
                        if event.content_block.type == "tool_use":
                            current_tool = {
                                "id": event.content_block.id,
                                "name": event.content_block.name,
                                "input": ""
                            }
                    elif event.type == "content_block_delta":
                        if hasattr(event.delta, "text"):
                            yield {"type": "text", "text": event.delta.text}
                        elif hasattr(event.delta, "partial_json"):
                            if current_tool:
                                current_tool["input"] += event.delta.partial_json
                    elif event.type == "content_block_stop":
                        if current_tool:
                            # Parse the accumulated JSON
                            import json
                            try:
                                current_tool["input"] = json.loads(current_tool["input"])
                            except json.JSONDecodeError:
                                current_tool["input"] = {}
                            yield {"type": "tool_use", "tool": current_tool}
                            current_tool = None
                    elif event.type == "message_stop":
                        final_message = await stream.get_final_message()
                        yield {
                            "type": "end",
                            "usage": {
                                "input_tokens": final_message.usage.input_tokens,
                                "output_tokens": final_message.usage.output_tokens
                            }
                        }

        except anthropic.APIError as e:
            logger.error(f"Anthropic API streaming error: {e}")
            yield {"type": "error", "error": str(e)}
        except Exception as e:
            logger.error(f"LLM streaming error: {e}")
            yield {"type": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_llm_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Get singleton LLM client instance."""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient(
            api_key=config.chat.anthropic_api_key,
            model=config.chat.model
        )
    return _llm_client
