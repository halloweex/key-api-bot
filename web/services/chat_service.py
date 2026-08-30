"""
Chat service for AI-powered sales analytics assistant.

Orchestrates LLM calls with tool execution for data-driven responses.
"""
import uuid
import json
from typing import Optional, List, Dict, Any, AsyncGenerator
from datetime import datetime
from dataclasses import dataclass, field

from core.llm_client import get_llm_client
from core.chat_tools import TOOLS, execute_tool
from core.observability import get_logger

logger = get_logger(__name__)


class ConversationAccessError(Exception):
    """A conversation id did not resolve to one owned by the caller.

    Raised for both a foreign conversation (owned by another user) and an
    unknown id. The route translates it to a 404 — deliberately not
    distinguishing the two, so a caller cannot probe which ids exist, and never
    silently creating a new conversation under an id the caller supplied.
    """


def _normalize_owner(user_id: Optional[Any]) -> Optional[str]:
    """Owner key. Session user_ids are ints; compare as strings throughout."""
    return None if user_id is None else str(user_id)


@dataclass
class Conversation:
    """Represents a chat conversation."""
    id: str
    messages: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)
    total_tokens: int = 0
    # The user who created the conversation. Every conversation is scoped to
    # its owner; a request carrying someone else's conversation_id is refused.
    user_id: Optional[str] = None


class ChatService:
    """Service for AI chat functionality."""

    # In-memory conversation store (simple, works for single instance)
    # For production scaling, use Redis or similar
    _conversations: Dict[str, Conversation] = {}

    def __init__(self):
        self.llm = get_llm_client()

    @property
    def is_available(self) -> bool:
        """Check if chat service is available."""
        return self.llm.is_available

    def create_conversation(
        self,
        context: Optional[Dict[str, Any]] = None,
        user_id: Optional[Any] = None,
    ) -> str:
        """
        Create a new conversation owned by ``user_id``.

        Args:
            context: Optional context (period, sales_type, language)
            user_id: The owner. Every conversation is scoped to its creator so
                one user cannot resume another user's conversation.

        Returns:
            Conversation ID
        """
        conv_id = f"conv_{uuid.uuid4().hex[:12]}"
        self._conversations[conv_id] = Conversation(
            id=conv_id,
            context=context or {},
            user_id=_normalize_owner(user_id),
        )
        logger.info(f"Created conversation {conv_id}")
        return conv_id

    def get_conversation(self, conv_id: str) -> Optional[Conversation]:
        """Get conversation by ID (no ownership check — internal use only)."""
        return self._conversations.get(conv_id)

    def resolve_owned(self, conv_id: str, user_id: Optional[Any]) -> Conversation:
        """Return the conversation only if ``user_id`` owns it.

        Raises ``ConversationAccessError`` when the id is unknown or is owned by
        someone else — the two are indistinguishable to the caller by design.
        This is the single ownership check every entry point routes through.

        An absent owner on either side is refused rather than matched. Under a
        plain ``==`` a conversation carrying no owner — the default of the
        dataclass field, and the shape any caller omitting ``user_id`` would
        produce — was returned to a caller whose own id was also None. No route
        can reach that today because ``_resolve_session`` rejects a session
        without a truthy user_id, but that is a guarantee in another module, and
        the safe direction for a conversation nobody owns is that nobody reaches
        it.
        """
        owner = _normalize_owner(user_id)
        conv = self._conversations.get(conv_id)
        if conv is None or owner is None or conv.user_id != owner:
            raise ConversationAccessError(conv_id)
        return conv

    async def chat(
        self,
        conv_id: str,
        message: str,
        user_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Send a message and get a response (non-streaming).

        Args:
            conv_id: Conversation ID
            message: User message
            user_id: The caller. Must own ``conv_id``.

        Returns:
            Response dict with content and metadata

        Raises:
            ConversationAccessError: ``conv_id`` is unknown or owned by someone
                else. The caller must create a conversation first (or omit the
                id at the route, which creates one for them); a foreign id is
                never silently adopted.
        """
        conv = self.resolve_owned(conv_id, user_id)

        # Add user message
        conv.messages.append({"role": "user", "content": message})

        # Get LLM response
        response = await self.llm.chat(
            messages=conv.messages,
            tools=TOOLS,
            max_tokens=1000
        )

        if response.get("error"):
            return {
                "conversation_id": conv_id,
                "content": response.get("content", "An error occurred"),
                "error": True
            }

        # Handle tool calls if present
        if response.get("tool_calls"):
            tool_results = await self._execute_tools(response["tool_calls"])

            # Add assistant message with tool calls (Anthropic format)
            # Content must be a list of content blocks
            assistant_content = []
            if response.get("content"):
                assistant_content.append({"type": "text", "text": response["content"]})
            for tool_call in response["tool_calls"]:
                assistant_content.append({
                    "type": "tool_use",
                    "id": tool_call["id"],
                    "name": tool_call["name"],
                    "input": tool_call["input"]
                })
            conv.messages.append({
                "role": "assistant",
                "content": assistant_content
            })

            # Add tool results as a single user message with all results
            tool_result_content = []
            for tool_result in tool_results:
                tool_result_content.append({
                    "type": "tool_result",
                    "tool_use_id": tool_result["tool_use_id"],
                    "content": json.dumps(tool_result["result"], ensure_ascii=False)
                })
            conv.messages.append({
                "role": "user",
                "content": tool_result_content
            })

            # Get follow-up response
            follow_up = await self.llm.chat(
                messages=conv.messages,
                tools=TOOLS,
                max_tokens=1000
            )

            final_content = follow_up.get("content", "")
            conv.total_tokens += follow_up.get("usage", {}).get("input_tokens", 0)
            conv.total_tokens += follow_up.get("usage", {}).get("output_tokens", 0)
        else:
            final_content = response.get("content", "")

        # Add final assistant message
        conv.messages.append({"role": "assistant", "content": final_content})

        # Update token count
        conv.total_tokens += response.get("usage", {}).get("input_tokens", 0)
        conv.total_tokens += response.get("usage", {}).get("output_tokens", 0)

        return {
            "conversation_id": conv_id,
            "content": final_content,
            "tokens_used": conv.total_tokens
        }

    async def chat_stream(
        self,
        conv_id: str,
        message: str,
        user_id: Optional[Any] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Send a message and stream the response.

        Args:
            conv_id: Conversation ID
            message: User message
            user_id: The caller. Must own ``conv_id``.

        Yields:
            Event dicts with streaming content

        Raises:
            ConversationAccessError: ``conv_id`` is unknown or owned by someone
                else. The route validates ownership before iterating so the
                client gets a 404, but the check is repeated here so the service
                is safe regardless of caller.
        """
        conv = self.resolve_owned(conv_id, user_id)

        # Add user message
        conv.messages.append({"role": "user", "content": message})

        # Stream LLM response
        accumulated_text = ""
        tool_calls = []

        async for event in self.llm.chat_stream(
            messages=conv.messages,
            tools=TOOLS,
            max_tokens=1000
        ):
            if event["type"] == "text":
                accumulated_text += event["text"]
                yield {"type": "chunk", "text": event["text"]}

            elif event["type"] == "tool_use":
                tool_calls.append(event["tool"])
                yield {
                    "type": "tool_call",
                    "tool": event["tool"]["name"],
                    "input": event["tool"]["input"]
                }

            elif event["type"] == "error":
                yield {"type": "error", "error": event["error"]}
                return

            elif event["type"] == "end":
                conv.total_tokens += event.get("usage", {}).get("input_tokens", 0)
                conv.total_tokens += event.get("usage", {}).get("output_tokens", 0)

        # Handle tool calls
        if tool_calls:
            # Add assistant message with tool calls (Anthropic format)
            assistant_content = []
            if accumulated_text:
                assistant_content.append({"type": "text", "text": accumulated_text})
            for tool_call in tool_calls:
                assistant_content.append({
                    "type": "tool_use",
                    "id": tool_call["id"],
                    "name": tool_call["name"],
                    "input": tool_call["input"]
                })
            conv.messages.append({
                "role": "assistant",
                "content": assistant_content
            })

            # Execute tools and add results
            tool_results = await self._execute_tools(tool_calls)
            tool_result_content = []
            for tool_result in tool_results:
                tool_result_content.append({
                    "type": "tool_result",
                    "tool_use_id": tool_result["tool_use_id"],
                    "content": json.dumps(tool_result["result"], ensure_ascii=False)
                })
                yield {
                    "type": "tool_result",
                    "tool": tool_result["tool_name"],
                    "result": tool_result["result"]
                }
            conv.messages.append({
                "role": "user",
                "content": tool_result_content
            })

            # Get follow-up response
            async for event in self.llm.chat_stream(
                messages=conv.messages,
                tools=TOOLS,
                max_tokens=1000
            ):
                if event["type"] == "text":
                    accumulated_text += event["text"]
                    yield {"type": "chunk", "text": event["text"]}
                elif event["type"] == "end":
                    conv.total_tokens += event.get("usage", {}).get("input_tokens", 0)
                    conv.total_tokens += event.get("usage", {}).get("output_tokens", 0)

        # Add final assistant message
        conv.messages.append({"role": "assistant", "content": accumulated_text})

        yield {
            "type": "end",
            "tokens_used": conv.total_tokens
        }

    async def _execute_tools(
        self,
        tool_calls: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Execute tool calls and return results."""
        results = []
        for tool_call in tool_calls:
            tool_name = tool_call.get("name", "")
            tool_input = tool_call.get("input", {})
            tool_id = tool_call.get("id", "")

            logger.info(f"Executing tool: {tool_name} with input: {tool_input}")

            result = await execute_tool(tool_name, tool_input)

            results.append({
                "tool_use_id": tool_id,
                "tool_name": tool_name,
                "result": result
            })

        return results

    def cleanup_old_conversations(self, max_age_hours: int = 24) -> int:
        """
        Remove old conversations to free memory.

        Args:
            max_age_hours: Max age in hours

        Returns:
            Number of conversations removed
        """
        from datetime import timedelta

        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        to_remove = [
            conv_id for conv_id, conv in self._conversations.items()
            if conv.created_at < cutoff
        ]

        for conv_id in to_remove:
            del self._conversations[conv_id]

        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} old conversations")

        return len(to_remove)


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

_chat_service: Optional[ChatService] = None


def get_chat_service() -> ChatService:
    """Get singleton chat service instance."""
    global _chat_service
    if _chat_service is None:
        _chat_service = ChatService()
    return _chat_service
