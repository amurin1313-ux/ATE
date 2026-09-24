# ATE FX Cloud Decision Relay

Репозиторий используется как транспорт и аудит между ATE FX и облачным портфельным арбитром.

Облако не создаёт стратегии и не отправляет ордера. ATE FX сначала формирует локальные BUY/SELL-кандидаты, затем GitHub Action может выбрать только существующий candidate_id или вернуть NO_TRADE/DEFER. После выбора кандидат всё равно проходит штатные news_guard, risk_engine, sizing, execution и reconcile.

## Секреты

В Settings → Secrets and variables → Actions добавьте Repository secret OPENAI_API_KEY.

Необязательно в Variables задайте ATE_FX_OPENAI_MODEL. По умолчанию workflow использует gpt-6-sol.

На рабочем ПК ATE FX нужен fine-grained GitHub token только для этого репозитория с Contents: Read and write. Он хранится в пользовательской переменной среды ATE_FX_GITHUB_TOKEN и никогда не коммитится.

## Файлы обмена

- ate_fx_cloud/requests/latest.json — обезличенный запрос ATE FX.
- ate_fx_cloud/decisions/latest.json — структурированное облачное решение.
- .github/workflows/ate-fx-cloud-decision.yml — обработчик push запроса.
- ate_fx_cloud/github_relay_worker.py — вызов OpenAI Responses API со Structured Outputs.

Торговые логины, номера брокерских тикетов, локальные пути и API-ключи в запрос не включаются.
