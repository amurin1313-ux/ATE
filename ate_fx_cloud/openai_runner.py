from __future__ import annotations
import json, os, sys, urllib.error, urllib.request
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from protocol import DECISION_JSON_SCHEMA, DECISION_SCHEMA_VERSION, read_json, atomic_json, utc_now, validate_decision

SYSTEM_PROMPT="""You are the portfolio arbitration layer for ATE FX DEMO trading.
The request contains only candidates already produced by local ATE FX model pipelines.
Select zero or more offered candidates only. Never invent a symbol, action, policy, candidate_key or signal.
Never set lots, monetary risk, stop-loss, take-profit, leverage or broker commands.
ATE FX remains final authority for risk, news, spread, reconcile, execution and current-signal validity.
Prefer NO_TRADE when evidence conflicts, data is stale or weak, correlation concentrates risk, or the offered edge is not convincing.
Consider cross-pair opportunity cost, probability versus native threshold, local BUY/SELL evidence, regime, volatility/spread, recent price structure, existing positions and available position slots.
Position views are advisory review labels only. Return only the requested JSON schema."""

def extract_text(payload):
    if isinstance(payload.get("output_text"),str) and payload["output_text"].strip(): return payload["output_text"]
    for item in payload.get("output",[]) if isinstance(payload.get("output"),list) else []:
        if not isinstance(item,dict): continue
        for content in item.get("content",[]) if isinstance(item.get("content"),list) else []:
            if isinstance(content,dict) and isinstance(content.get("text"),str): return content["text"]
    return ""

def call_openai(request_payload):
    key=os.environ.get("OPENAI_API_KEY","").strip()
    if not key: raise RuntimeError("OPENAI_API_KEY_MISSING")
    body={
      "model":os.environ.get("ATE_FX_OPENAI_MODEL","gpt-6-sol"),
      "reasoning":{"effort":os.environ.get("ATE_FX_REASONING_EFFORT","high")},
      "input":[
        {"role":"system","content":[{"type":"input_text","text":SYSTEM_PROMPT}]},
        {"role":"user","content":[{"type":"input_text","text":json.dumps(request_payload,ensure_ascii=False,separators=(",",":"))}]}
      ],
      "text":{"format":{"type":"json_schema","name":"ate_fx_portfolio_decision","strict":True,"schema":DECISION_JSON_SCHEMA}},
      "max_output_tokens":3500
    }
    req=urllib.request.Request("https://api.openai.com/v1/responses",data=json.dumps(body,ensure_ascii=False).encode("utf-8"),method="POST",
      headers={"Authorization":f"Bearer {key}","Content-Type":"application/json","User-Agent":"ATE-FX-GitHub-Relay/1.0"})
    try:
        with urllib.request.urlopen(req,timeout=90) as resp: response=json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"OPENAI_HTTP_{exc.code}:{exc.read().decode('utf-8',errors='replace')[:3000]}") from exc
    text=extract_text(response)
    if not text: raise RuntimeError("OPENAI_EMPTY_STRUCTURED_OUTPUT")
    decision=json.loads(text);decision.setdefault("schema_version",DECISION_SCHEMA_VERSION)
    return decision,{"response_id":response.get("id"),"model":response.get("model"),"usage":response.get("usage") or {},"received_utc":utc_now()}

def main():
    request_path=Path("ate_fx_cloud_exchange/request/latest_request.json")
    decision_path=Path("ate_fx_cloud_exchange/decision/latest_decision.json")
    request=read_json(request_path)
    if not request: raise SystemExit("request missing or invalid")
    decision,meta=call_openai(request)
    ok,reason,clean=validate_decision(decision,request,int(os.environ.get("ATE_FX_MAX_SELECTED","2")))
    if not ok: raise SystemExit("decision invalid: "+reason)
    clean["openai_meta"]=meta
    atomic_json(decision_path,clean)
    print(json.dumps({"status":"OK","request_id":clean.get("request_id"),"decision_id":clean.get("decision_id")},ensure_ascii=False))
if __name__=="__main__": main()
