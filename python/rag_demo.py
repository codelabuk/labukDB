"""
Minimal RAG demo: plain-English question -> local Ollama writes the same
JSON query_demo.py sends by hand -> real Flight query against the running
server -> Ollama answers the question using only the retrieved rows.

Two Ollama calls, two different jobs:
  1. question + table schema  -> JSON predicate (retrieval planning)
  2. question + retrieved rows -> natural-language answer (generation)

That split IS what "RAG" means here: step 1 is retrieval (grounded in the
real table, via the same pruning/filtering engine as every other client),
step 2 is generation (grounded in step 1's actual results, not the model's
own guess).

Requires: pip install pyarrow
Requires: Ollama running locally (https://ollama.com) with a model pulled,
          e.g. `ollama pull qwen2.5-coder`

Usage:
    python rag_demo.py <host:port> <table-name-or-tableRoot> "<question>" [--model MODEL]

Example:
    python rag_demo.py localhost:9443 logs "how many errors came from payments-svc?" --model qwen2.5-coder

KNOWN LIMITATION, stated plainly: string comparisons are case-sensitive
(see PredicateEvaluation) and the LLM has no way to know your data's exact
casing (e.g. "ERROR" vs "error") — it can only guess from the question's
wording. A real fix would fetch distinct values for low-cardinality
columns and include them as hints in the prompt; not attempted here.

Also defends against one real, observed small-model mistake: using the
comparison operator itself (e.g. "eq") as the JSON "type" field instead of
the literal string "cmp" the wire format requires. See normalize_predicate()
— it repairs this specific pattern and prints when it does, rather than
either crashing or silently masking what the model actually produced.
"""

import json
import re
import sys
import urllib.error
import urllib.request

import pyarrow.flight as flight

OLLAMA_HOST = "http://localhost:11434"
DEFAULT_MODEL = "qwen2.5-coder"

PREDICATE_GRAMMAR = """Predicate grammar (JSON):
- Comparison: {"type":"cmp","column":"<col>","op":"EQ|NEQ|LT|LTE|GT|GTE","literal":<value>,"literalType":"string|long|double|timestamp"}
- IN:         {"type":"in","column":"<col>","literals":[<value>,...],"literalType":"string|long|double"}
- LIKE:       {"type":"like","column":"<col>","pattern":"<sql-like-pattern-with-%-or-_>"}
- AND/OR:     {"type":"and","children":[<predicate>,...]}  or  {"type":"or","children":[...]}
timestamp literals must be ISO-8601, e.g. "2026-07-17T00:00:00Z"."""


def call_ollama(prompt, model, json_mode=False):
    payload = {"model": model, "prompt": prompt, "stream": False}
    if json_mode:
        payload["format"] = "json"
    req = urllib.request.Request(
        f"{OLLAMA_HOST}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        return body["response"]
    except urllib.error.HTTPError as e:
        # Ollama returns 404 with a JSON {"error": "..."} body for "model
        # not found" specifically, not just for a bad URL — surface that
        # instead of a raw traceback, since it's almost always the actual
        # problem here.
        detail = e.read().decode("utf-8", errors="replace")
        print(f"Ollama returned HTTP {e.code}: {detail}")
        print()
        print(f"Most likely: '{model}' isn't pulled. Run `ollama list` to see what "
              f"you have, or `ollama pull {model}` to get it.")
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Could not reach Ollama at {OLLAMA_HOST}: {e}")
        print("Is `ollama serve` running?")
        sys.exit(1)


def extract_json(text):
    """Models sometimes wrap JSON in prose or markdown fences despite format=json; salvage the first {...} block if direct parsing fails."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise ValueError(f"Model response contained no JSON object:\n{text}")
        return json.loads(match.group(0))


def literal_type_hint(arrow_type):
    import pyarrow as pa
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    if pa.types.is_integer(arrow_type):
        return "long"
    if pa.types.is_floating(arrow_type):
        return "double"
    return "string"  # covers Utf8 and the fallback case


def fetch_schema(client, table):
    descriptor = flight.FlightDescriptor.for_command(json.dumps({"table": table}).encode("utf-8"))
    info = client.get_flight_info(descriptor)
    return info.schema


def generate_query(question, schema, model):
    schema_lines = "\n".join(f"  {f.name}: {literal_type_hint(f.type)}" for f in schema)
    prompt = f"""You are translating a plain-English question into a JSON filter for a data query API.

Table schema (column: type-hint):
{schema_lines}

Only reference columns listed above. Respond with ONLY a JSON object, no explanation, no markdown fences:
{{"predicate": <predicate-or-null>, "columns": <array-of-column-names-or-null>}}

{PREDICATE_GRAMMAR}

The "type" field must be EXACTLY one of: "cmp", "in", "like", "and", "or" — never the
operator name itself (e.g. a comparison's type is "cmp", not "eq", even when its "op" is "EQ").
"columns" always belongs at the TOP level of the response object, never inside the predicate.

Worked example — question "errors from payments-svc":
{{"predicate": {{"type": "and", "children": [
  {{"type": "cmp", "column": "service", "op": "EQ", "literal": "payments-svc", "literalType": "string"}},
  {{"type": "cmp", "column": "level", "op": "EQ", "literal": "ERROR", "literalType": "string"}}
]}}, "columns": null}}

If the question needs no filter, use "predicate": null.
If the question doesn't specify which columns to return, use "columns": null.

Question: {question}"""
    raw = call_ollama(prompt, model, json_mode=True)
    parsed = extract_json(raw)
    if parsed.get("predicate") is not None:
        parsed["predicate"] = normalize_predicate(parsed["predicate"])
    return parsed


def normalize_predicate(node):
    """
    Defensive repair for a real, observed small-model failure mode: using
    the comparison operator itself (e.g. "eq") as the "type" field instead
    of the literal string "cmp" the wire format actually requires. Rather
    than trust the model's output blindly and let the server reject it,
    this recognizes "has column/op/literal but an unrecognized type" as a
    comparison node and repairs it — printing a warning so the repair is
    visible, not silently masking what the model actually produced.
    """
    if not isinstance(node, dict):
        return node

    node_type = node.get("type")
    if node_type in ("cmp", "in", "like"):
        return node
    if node_type in ("and", "or"):
        return {"type": node_type, "children": [normalize_predicate(c) for c in node.get("children", [])]}

    if "column" in node and "op" in node and "literal" in node:
        print(f"  (repairing malformed predicate node: type was '{node_type}', normalized to 'cmp')")
        repaired = {"type": "cmp", "column": node["column"], "op": node["op"], "literal": node["literal"]}
        if "literalType" in node:
            repaired["literalType"] = node["literalType"]
        return repaired

    raise ValueError(f"Could not normalize predicate node, model output too malformed to repair: {node}")


def run_query(client, table, query_spec):
    body = {"table": table}
    body.update(query_spec)
    descriptor = flight.FlightDescriptor.for_command(json.dumps(body).encode("utf-8"))
    info = client.get_flight_info(descriptor)

    tables = []
    for endpoint in info.endpoints:
        reader = client.do_get(endpoint.ticket)
        tables.append(reader.read_all())

    if not tables:
        return None
    combined = tables[0]
    for t in tables[1:]:
        combined = pa_concat(combined, t)
    return combined


def pa_concat(a, b):
    import pyarrow as pa
    return pa.concat_tables([a, b])


def generate_answer(question, query_spec, table, model, preview_rows=20):
    row_count = table.num_rows if table is not None else 0
    preview = table.slice(0, preview_rows).to_pylist() if table is not None else []
    prompt = f"""You answered a data question by running a query and got these results.

Question: {question}

Query executed (JSON): {json.dumps(query_spec)}

Result count: {row_count} rows
Sample rows (up to {preview_rows}): {json.dumps(preview, default=str)}

Write a concise, direct answer to the question based only on this data. If the data doesn't answer it, say so plainly."""
    return call_ollama(prompt, model, json_mode=False)


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)

    location = f"grpc://{sys.argv[1]}"
    table = sys.argv[2]
    question = sys.argv[3]
    model = DEFAULT_MODEL
    if "--model" in sys.argv:
        model = sys.argv[sys.argv.index("--model") + 1]

    client = flight.FlightClient(location)

    print(f"Fetching schema for '{table}'...")
    schema = fetch_schema(client, table)

    print(f"Asking {model} to translate the question into a query...")
    query_spec = generate_query(question, schema, model)
    print(f"Generated query: {json.dumps(query_spec)}")
    print()

    print("Running the query...")
    result = run_query(client, table, query_spec)
    row_count = result.num_rows if result is not None else 0
    print(f"Retrieved {row_count} row(s).")
    print()

    print(f"Asking {model} to answer using the retrieved data...")
    answer = generate_answer(question, query_spec, result, model)
    print()
    print("=== Answer ===")
    print(answer.strip())


if __name__ == "__main__":
    main()
