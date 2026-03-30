"""Config endpoints."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

import web.server as srv

router = APIRouter()


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class ConfigPatch(BaseModel):
    """Update a single configuration parameter."""
    key: str = Field(..., description="Configuration key to update.", examples=["max_memtable_size"])
    value: int | float | str = Field(..., description="New value for the parameter.", examples=[4194304])


class ConfigPatchResponse(BaseModel):
    """Response after updating a config parameter."""
    ok: bool = Field(..., description="Whether the update succeeded.")
    key: str = Field(..., description="The configuration key that was updated.")
    old_value: int | float | str = Field(..., description="Previous value.")
    new_value: int | float | str = Field(..., description="New value after update.")


class ConfigField(BaseModel):
    """A single field in the config schema."""
    key: str = Field(..., description="Configuration key name.")
    type: str = Field(..., description="Python type name (int, float, str).")
    value: int | float | str = Field(..., description="Current value.")


class ConfigSchemaResponse(BaseModel):
    """Full config schema with types and current values."""
    fields: list[ConfigField] = Field(..., description="All configuration fields.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", summary="Get current configuration")
async def config_get() -> dict[str, object]:
    """Return all configuration parameters as a flat JSON object."""
    e = srv.get_engine()
    return e.config.to_dict()


@router.patch("", response_model=ConfigPatchResponse, summary="Update a config parameter")
async def config_patch(req: ConfigPatch) -> dict[str, object]:
    """Update a single configuration parameter at runtime.

    Returns both the old and new values for confirmation.
    """
    e = srv.get_engine()
    old, new = e.update_config(req.key, req.value)
    return {"ok": True, "key": req.key, "old_value": old, "new_value": new}


@router.get("/schema", response_model=ConfigSchemaResponse, summary="Get config schema")
async def config_schema() -> dict[str, object]:
    """Return the config schema with field names, types, and current values."""
    e = srv.get_engine()
    current = e.config.to_dict()
    schema: list[dict[str, object]] = []
    for key, value in current.items():
        schema.append({
            "key": key,
            "type": type(value).__name__,
            "value": value,
        })
    return {"fields": schema}
