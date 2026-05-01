from __future__ import annotations

import argparse
import json
import sys

from analytics_assistant.orchestrator import AnalyticsOrchestrator


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Ask the analytics assistant a natural-language question.")
    parser.add_argument("question")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM summarization and print raw evidence summary.")
    parser.add_argument("--json", action="store_true", help="Return the full orchestrator payload as JSON.")
    args = parser.parse_args()

    orchestrator = AnalyticsOrchestrator()
    payload = orchestrator.answer(args.question, top_k=args.top_k, use_llm=not args.no_llm)

    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(payload["answer"])


if __name__ == "__main__":
    main()
