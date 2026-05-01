Set-Location -LiteralPath $PSScriptRoot
python -m uvicorn analytics_assistant.app:app --host 127.0.0.1 --port 8000 *> storage/ui.log
