"""Export the OpenAPI schema to a static JSON file for the docs site."""

import json
from pathlib import Path

from web.server import app

out = Path(__file__).resolve().parent.parent / "docs" / "assets" / "openapi.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(app.openapi(), indent=2))
print(f"Wrote {out} ({out.stat().st_size} bytes)")
