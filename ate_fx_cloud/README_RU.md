# ATE FX Cloud AI Relay

Контур для ATE FX v5.0.73. Репозиторий публичный, поэтому сюда допускается только обезличенный request: без логина счёта, ticket id, API-ключей и абсолютного баланса.

## Поток
1. Локальный ATE FX формирует sanitized request и пишет `ate_fx_cloud_exchange/request/latest_request.json`.
2. Workflow запускается только при изменении request.
3. OpenAI Responses API возвращает Structured Output.
4. `protocol.py` проверяет, что каждый selected entry существовал в исходном request и совпадает по symbol/action/policy.
5. Валидированное решение записывается в `ate_fx_cloud_exchange/decision/latest_decision.json`.
6. Локальный ATE FX может использовать его только как portfolio selector поверх уже валидных локальных DEMO-кандидатов. Жёсткие ATE FX risk/news/spread/reconcile/execution gates остаются последней инстанцией.

## Настройка GitHub
В Settings → Secrets and variables → Actions создать secret `OPENAI_API_KEY`.
Опциональные variables: `ATE_FX_OPENAI_MODEL=gpt-5.6`, `ATE_FX_REASONING_EFFORT=high`, `ATE_FX_MAX_SELECTED=2`.

Этот relay не предназначен для тикового low-latency исполнения; для оперативного контура локальная программа поддерживает DIRECT_OPENAI.
