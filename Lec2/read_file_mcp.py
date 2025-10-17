from mcp.server.fastmcp import FastMCP
import json
import base64
from pathlib import Path
from typing import Optional
import pandas as pd

mcp = FastMCP("FileReader", port=8011)

def _p(path: str) -> Path:
    # Очень простая нормализация пути (без спец. защиты)
    return Path(path).expanduser().resolve()

@mcp.tool()
def ls(path: str=".", pattern: Optional[str]=None, max_items: int=100) -> list[dict]:
    """
    List directory items with basic metadata.
    """
    p = _p(path)
    items = []
    if p.is_dir():
        for i, child in enumerate(sorted(p.iterdir())):
            if i >= max_items:
                break
            name = child.name
            if pattern and pattern not in name:
                continue
            try:
                st = child.stat()
                items.append({
                    "name": name,
                    "path": str(child),
                    "is_dir": child.is_dir(),
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                })
            except Exception as e:
                items.append({
                    "name": name,
                    "path": str(child),
                    "error": str(e)
                })
    else:
        raise FileNotFoundError(f"Not a directory: {p}")
    return items

@mcp.tool()
def stat(path: str) -> dict:
    """
    Return basic file info.
    """
    p = _p(path)
    st = p.stat()
    return {
        "path": str(p),
        "exists": p.exists(),
        "is_dir": p.is_dir(),
        "size": st.st_size,
        "mtime": st.st_mtime,
    }

@mcp.tool()
def read_text(path: str, encoding: str="utf-8", n: Optional[int]=None) -> dict:
    """
    Read text file (full or first n characters).
    """
    p = _p(path)
    with open(p, "r", encoding=encoding) as f:
        data = f.read()
    if n is not None:
        data = data[:n]
    return {"path": str(p), "text": data}

@mcp.tool()
def read_bytes_b64(path: str, n: Optional[int]=None) -> dict:
    """
    Read binary file and return base64 string (full or first n bytes).
    """
    p = _p(path)
    with open(p, "rb") as f:
        b = f.read()
    if n is not None:
        b = b[:n]
    return {"path": str(p), "b64": base64.b64encode(b).decode("ascii")}

@mcp.tool()
def head_csv(path: str, n: int=5) -> list[dict]:
    """
    Return first n rows of CSV as list of dicts.
    """
    p = _p(path)
    # читаем только нужные строки для скорости
    df = pd.read_csv(p, nrows=n)
    # заменяем NaN на None и обеспечиваем JSON-friendly значения
    payload = json.loads(df.where(pd.notnull(df), None).to_json(orient="records", date_format="iso"))
    return payload

@mcp.tool()
def read_json(path: str) -> dict | list:
    """
    Read JSON file and return parsed data.
    """
    p = _p(path)
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
