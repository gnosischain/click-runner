#!/usr/bin/env python3
import argparse, os
from pathlib import Path
import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def build_context(config: dict, args: argparse.Namespace) -> dict:
    ctx = dict(config)
    # Prefer click-runner style; fallback to DUNE_API_KEY if used locally
    dune_api_key = (os.environ.get("CH_QUERY_VAR_DUNE_API_KEY")
                    or os.environ.get("DUNE_API_KEY") or "")
    ctx.setdefault("dune_api_key", dune_api_key)
    # Expose any CH_QUERY_VAR_* into context (handy for START_DATE/END_DATE etc.)
    for k, v in os.environ.items():
        if k.startswith("CH_QUERY_VAR_"):
            key = k[len("CH_QUERY_VAR_"):].lower()
            ctx.setdefault(key, v)
            ctx.setdefault(k, v)
    # Optional CLI overrides
    if args.override:
        for pair in args.override:
            if "=" not in pair:
                raise SystemExit(f"--override expects KEY=VALUE, got: {pair}")
            k, v = pair.split("=", 1)
            ctx[k] = v
    return ctx

def render_templates(templates_dir: Path, out_dir: Path, context: dict):
    env = Environment(loader=FileSystemLoader(str(templates_dir)),
                      undefined=StrictUndefined, autoescape=False,
                      trim_blocks=True, lstrip_blocks=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    rendered = []
    for tpl in templates_dir.rglob("*.sql.j2"):
        rel = tpl.relative_to(templates_dir)
        out_path = out_dir / rel.with_suffix("")  # strip .j2 -> .sql
        out_path.parent.mkdir(parents=True, exist_ok=True)
        sql = env.get_template(str(rel)).render(**context)
        out_path.write_text(sql, encoding="utf-8")
        rendered.append(out_path)
    return rendered

def main():
    p = argparse.ArgumentParser(description="Render Dune→ClickHouse SQL templates")
    p.add_argument("--templates-dir", default="queries/dune/_templates")
    p.add_argument("--dataset-dir", required=True)
    p.add_argument("--out-dir")
    p.add_argument("--override", nargs="*")
    args = p.parse_args()

    templates = Path(args.templates_dir).resolve()
    dataset = Path(args.dataset_dir).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else dataset

    cfg = dataset / "config.yml"
    if not cfg.exists(): raise SystemExit(f"Missing {cfg}")
    if not templates.exists(): raise SystemExit(f"Templates dir not found: {templates}")

    ctx = build_context(load_yaml(cfg), args)
    required = [
        "dune_query_id_full","csv_schema","select_list",
        "target_table","columns_ddl","partition_expr","order_by_expr","target_columns",
    ]
    missing = [k for k in required if not ctx.get(k)]
    if missing: raise SystemExit(f"Missing keys in {cfg}: {missing}")

    paths = render_templates(templates, out_dir, ctx)
    print("Rendered:")
    for pth in paths: print(" -", pth)

if __name__ == "__main__":
    main()