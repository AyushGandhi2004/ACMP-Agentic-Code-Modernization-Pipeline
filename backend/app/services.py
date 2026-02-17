import json
import asyncio
from typing import AsyncGenerator
from acmp.graph import graph

async def run_modernization_stream(file_name: str, code: str) -> AsyncGenerator[str, None]:
    """Streams graph updates for a single uploaded file string."""
    
    # Initial state using the code provided by the frontend
    state = {
        "file_path": file_name,
        "original_code": code,
        "transformation_plan": None,
        "current_code": None,
        "error_logs": None,
        "itr": 0,
    }

    try:
        # Stream node updates using LangGraph
        async for chunk in graph.astream(state, stream_mode="updates"):
            for node_name, node_data in chunk.items():
                payload = {
                    "node": node_name,
                    "file_path": file_name,
                    "current_code": node_data.get("current_code"),
                    "error_logs": node_data.get("error_logs"),
                    "original_code": code if node_name == "auditor" else None
                }
                yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(0.1)
    except Exception as e:
        yield f"data: {json.dumps({'error': str(e)})}\n\n"