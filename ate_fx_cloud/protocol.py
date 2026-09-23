from __future__ import annotations
import hashlib, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DECISION_SCHEMA_VERSION = "ate_fx_cloud.portfolio_decision.v1"

def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def stable_hash(value: Any) -> str:
    raw=json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"),default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def read_json(path: Path) -> dict[str,Any]:
    try:
        obj=json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj,dict) else {}
    except Exception:
        return {}

def atomic_json(path: Path,payload: dict[str,Any]) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name("."+path.name+".tmp")
    tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str)+"\n",encoding="utf-8")
    tmp.replace(path)

DECISION_JSON_SCHEMA={
  "type":"object","additionalProperties":False,
  "required":["schema_version","decision_id","request_id","input_snapshot_hash","generated_utc","ttl_seconds","overall_action","selected_entries","position_views","market_regime_summary","global_reason_codes","data_quality_flags"],
  "properties":{
    "schema_version":{"type":"string","enum":[DECISION_SCHEMA_VERSION]},
    "decision_id":{"type":"string"},
    "request_id":{"type":"string"},
    "input_snapshot_hash":{"type":"string"},
    "generated_utc":{"type":"string"},
    "ttl_seconds":{"type":"integer","minimum":15,"maximum":300},
    "overall_action":{"type":"string","enum":["SELECT_ENTRIES","NO_TRADE","MANAGE_ONLY","HOLD"]},
    "selected_entries":{"type":"array","maxItems":4,"items":{"type":"object","additionalProperties":False,
      "required":["candidate_key","signal_id","symbol","action","policy_id","priority","confidence","reason_codes","reason"],
      "properties":{
        "candidate_key":{"type":"string"},"signal_id":{"type":"string"},"symbol":{"type":"string"},
        "action":{"type":"string","enum":["BUY","SELL"]},"policy_id":{"type":"string"},
        "priority":{"type":"integer","minimum":1,"maximum":4},"confidence":{"type":"number","minimum":0,"maximum":1},
        "reason_codes":{"type":"array","maxItems":8,"items":{"type":"string"}},"reason":{"type":"string","maxLength":600}
      }}},
    "position_views":{"type":"array","maxItems":8,"items":{"type":"object","additionalProperties":False,
      "required":["symbol","action","confidence","reason_codes","reason"],
      "properties":{"symbol":{"type":"string"},"action":{"type":"string","enum":["HOLD","EXIT_REVIEW","TRAIL_REVIEW"]},
      "confidence":{"type":"number","minimum":0,"maximum":1},"reason_codes":{"type":"array","maxItems":8,"items":{"type":"string"}},
      "reason":{"type":"string","maxLength":600}}}},
    "market_regime_summary":{"type":"string","maxLength":1000},
    "global_reason_codes":{"type":"array","maxItems":12,"items":{"type":"string"}},
    "data_quality_flags":{"type":"array","maxItems":12,"items":{"type":"string"}}
  }
}

def validate_decision(decision: Any, request: dict[str,Any], max_selected: int=2) -> tuple[bool,str,dict[str,Any]]:
    if not isinstance(decision,dict): return False,"DECISION_NOT_OBJECT",{}
    if decision.get("schema_version")!=DECISION_SCHEMA_VERSION: return False,"DECISION_SCHEMA_MISMATCH",{}
    if str(decision.get("request_id") or "")!=str(request.get("request_id") or ""): return False,"REQUEST_ID_MISMATCH",{}
    offered={str(c.get("candidate_key")):c for c in request.get("candidates",[]) if isinstance(c,dict)}
    selected=[];seen=set()
    for item in decision.get("selected_entries",[]) if isinstance(decision.get("selected_entries"),list) else []:
        if not isinstance(item,dict): return False,"SELECTED_ENTRY_NOT_OBJECT",{}
        key=str(item.get("candidate_key") or ""); src=offered.get(key)
        if not src: return False,f"UNOFFERED_CANDIDATE:{key}",{}
        if key in seen: continue
        if str(item.get("symbol") or "").upper()!=str(src.get("symbol") or "").upper(): return False,f"CANDIDATE_SYMBOL_MISMATCH:{key}",{}
        if str(item.get("action") or "").upper()!=str(src.get("action") or "").upper(): return False,f"CANDIDATE_ACTION_MISMATCH:{key}",{}
        if str(item.get("policy_id") or "")!=str(src.get("policy_id") or ""): return False,f"CANDIDATE_POLICY_MISMATCH:{key}",{}
        item=dict(item); item["source_signal_id"]=str(src.get("signal_id") or ""); item["source_features_hash"]=str(src.get("features_hash") or "")
        selected.append(item);seen.add(key)
    slots=max(0,int((request.get("risk_envelope") or {}).get("available_position_slots") or 0))
    cap=max(0,min(int(max_selected),slots if slots>0 else int(max_selected)))
    out=dict(decision);out["selected_entries"]=sorted(selected,key=lambda x:int(x.get("priority") or 99))[:cap]
    out["validated_utc"]=utc_now();out["validation_status"]="PASS";out["direct_trading_allowed"]=False;out["ate_fx_risk_gate_required"]=True
    out["decision_hash"]="sha256:"+stable_hash(out)
    return True,"OK",out
