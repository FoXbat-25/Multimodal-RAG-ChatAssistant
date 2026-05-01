from __future__ import annotations

from typing import Any

try:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse
    from pydantic import BaseModel
except ModuleNotFoundError as exc:  # pragma: no cover
    raise RuntimeError(
        "FastAPI is not installed. Install dependencies with `pip install -r requirements.txt`."
    ) from exc

from analytics_assistant.orchestrator import AnalyticsOrchestrator
from analytics_assistant.registry import build_gateway


app = FastAPI(title="Internal Analytics Assistant Tool API")
gateway = build_gateway()
orchestrator = AnalyticsOrchestrator(gateway=gateway)


class AskRequest(BaseModel):
    question: str
    top_k: int = 5
    use_llm: bool = True


@app.get("/", response_class=HTMLResponse)
def ui() -> str:
    return _HTML_PAGE


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/tools/{tool_name}")
def call_tool(tool_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    return gateway.call(tool_name, **payload)


@app.post("/ask")
def ask(payload: AskRequest) -> dict[str, Any]:
    return orchestrator.answer(
        payload.question,
        top_k=payload.top_k,
        use_llm=payload.use_llm,
    )


_HTML_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Internal Analytics Assistant</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f7f4;
      --panel: #ffffff;
      --text: #202124;
      --muted: #62665f;
      --line: #d8d8d0;
      --accent: #0f766e;
      --accent-dark: #115e59;
      --warn: #8a4b00;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
    }
    .shell {
      display: grid;
      grid-template-rows: auto 1fr auto;
      min-height: 100vh;
    }
    header {
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      padding: 14px 22px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    h1 {
      font-size: 18px;
      line-height: 1.2;
      margin: 0;
      font-weight: 650;
    }
    .status {
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--accent);
      display: inline-block;
    }
    main {
      width: min(1120px, 100%);
      margin: 0 auto;
      padding: 18px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) 320px;
      gap: 18px;
    }
    .conversation, aside {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      min-height: 0;
    }
    .conversation {
      display: grid;
      grid-template-rows: 1fr auto;
      height: calc(100vh - 112px);
    }
    #messages {
      overflow: auto;
      padding: 18px;
    }
    .message {
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      margin-bottom: 12px;
      background: #fbfbf8;
      white-space: pre-wrap;
      line-height: 1.45;
      font-size: 14px;
    }
    .message.user {
      background: #edf7f5;
      border-color: #b8d9d4;
    }
    .meta {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 8px;
    }
    form {
      border-top: 1px solid var(--line);
      padding: 14px;
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      background: var(--panel);
      border-radius: 0 0 8px 8px;
    }
    textarea {
      resize: none;
      min-height: 46px;
      max-height: 140px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      font: inherit;
      line-height: 1.35;
      color: var(--text);
    }
    button {
      border: 0;
      border-radius: 8px;
      background: var(--accent);
      color: white;
      padding: 0 18px;
      font: inherit;
      font-weight: 650;
      cursor: pointer;
      min-width: 92px;
    }
    button:hover { background: var(--accent-dark); }
    button:disabled { opacity: 0.6; cursor: progress; }
    aside {
      padding: 16px;
      height: calc(100vh - 112px);
      overflow: auto;
    }
    aside h2 {
      font-size: 14px;
      margin: 0 0 10px;
      font-weight: 700;
    }
    .control {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 0;
      border-bottom: 1px solid var(--line);
      color: var(--muted);
      font-size: 13px;
    }
    .control input[type="number"] {
      width: 68px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 6px;
    }
    .source-list {
      margin-top: 16px;
      display: grid;
      gap: 8px;
    }
    .source {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 9px;
      font-size: 12px;
      color: var(--muted);
      overflow-wrap: anywhere;
      background: #fbfbf8;
    }
    .source strong {
      color: var(--text);
    }
    .error {
      color: #8a1f11;
      background: #fff4ef;
      border-color: #f0c6b8;
    }
    @media (max-width: 820px) {
      main { grid-template-columns: 1fr; padding: 12px; }
      .conversation, aside { height: auto; }
      .conversation { min-height: 68vh; }
      header { padding: 12px; align-items: flex-start; flex-direction: column; }
      form { grid-template-columns: 1fr; }
      button { min-height: 42px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>Internal Analytics Assistant</h1>
      <div class="status"><span class="dot"></span><span>Tool-only data access</span></div>
    </header>
    <main>
      <section class="conversation" aria-label="Conversation">
        <div id="messages">
          <div class="message">
            <div class="meta"><strong>Assistant</strong><span>ready</span></div>
            Ask a question about your indexed documents and spreadsheets.
          </div>
        </div>
        <form id="ask-form">
          <textarea id="question" placeholder="Which comedy movies failed based on low rating?" required></textarea>
          <button id="send" type="submit">Ask</button>
        </form>
      </section>
      <aside aria-label="Sources">
        <h2>Controls</h2>
        <label class="control"><span>Use LLM</span><input id="use-llm" type="checkbox" checked></label>
        <label class="control"><span>Top document chunks</span><input id="top-k" type="number" min="1" max="10" value="5"></label>
        <h2 style="margin-top:18px;">Last Sources</h2>
        <div id="sources" class="source-list"></div>
      </aside>
    </main>
  </div>
  <script>
    const form = document.querySelector("#ask-form");
    const messages = document.querySelector("#messages");
    const sources = document.querySelector("#sources");
    const button = document.querySelector("#send");
    const textarea = document.querySelector("#question");

    function addMessage(role, text, extraClass = "") {
      const item = document.createElement("div");
      item.className = `message ${role === "You" ? "user" : ""} ${extraClass}`;
      item.innerHTML = `<div class="meta"><strong>${role}</strong><span>${new Date().toLocaleTimeString()}</span></div>`;
      item.append(document.createTextNode(text));
      messages.append(item);
      messages.scrollTop = messages.scrollHeight;
    }

    function renderSources(items) {
      sources.innerHTML = "";
      if (!items || !items.length) {
        sources.innerHTML = "<div class='source'>No sources returned.</div>";
        return;
      }
      for (const source of items.slice(0, 12)) {
        const row = source.row ? " | " + Object.entries(source.row).slice(0, 4).map(([k, v]) => `${k}: ${v}`).join("; ") : "";
        const detail = source.type === "document"
          ? `${source.name}${source.page ? ", page " + source.page : ""}`
          : `${source.name}${source.rank_by ? " | " + source.rank_by + "=" + source.rank_value : ""}${row}`;
        const item = document.createElement("div");
        item.className = "source";
        item.innerHTML = `<strong>[${source.id}]</strong> ${source.type}<br>${detail}`;
        sources.append(item);
      }
    }

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const question = textarea.value.trim();
      if (!question) return;
      addMessage("You", question);
      textarea.value = "";
      button.disabled = true;
      button.textContent = "Working";
      try {
        const response = await fetch("/ask", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            question,
            top_k: Number(document.querySelector("#top-k").value || 5),
            use_llm: document.querySelector("#use-llm").checked
          })
        });
        if (!response.ok) {
          throw new Error(await response.text());
        }
        const payload = await response.json();
        addMessage("Assistant", payload.answer);
        renderSources(payload.sources || []);
      } catch (error) {
        addMessage("Assistant", String(error), "error");
      } finally {
        button.disabled = false;
        button.textContent = "Ask";
        textarea.focus();
      }
    });
  </script>
</body>
</html>
"""
