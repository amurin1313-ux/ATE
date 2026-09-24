from __future__ import annotations
import hashlib, json, os, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUEST = Path(os.environ.get('ATE_FX_CLOUD_REQUEST_PATH', 'ate_fx_cloud/requests/latest.json'))
DECISION = Path(os.environ.get('ATE_FX_CLOUD_DECISION_PATH', 'ate_fx_cloud/decisions/latest.json'))
MODEL = os.environ.get('ATE_FX_OPENAI_MODEL', 'gpt-6-sol')
TTL = int(os.environ.get('ATE_FX_DECISION_TTL_SECONDS', '240'))

def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')

def stable(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj,sort_keys=True,ensure_ascii=False,separators=(',',':'),default=str).encode()).hexdigest()

def schema(ids: list[str]) -> dict[str, Any]:
    enums=ids+(['NONE'] if 'NONE' not in ids else [])
    return {
      'type':'object','properties':{
        'verdict':{'type':'string','enum':['SELECT','NO_TRADE','DEFER']},
        'selected':{'type':'object','properties':{'candidate_id':{'type':'string','enum':enums},'reason':{'type':'string'}},'required':['candidate_id','reason'],'additionalProperties':False},
        'ranking':{'type':'array','items':{'type':'object','properties':{
            'candidate_id':{'type':'string','enum':enums},'decision':{'type':'string','enum':['ALLOW','BLOCK']},
            'priority':{'type':'number','minimum':0,'maximum':100},'confidence':{'type':'number','minimum':0,'maximum':1},
            'reason_codes':{'type':'array','items':{'type':'string'}},'reason':{'type':'string'}},
            'required':['candidate_id','decision','priority','confidence','reason_codes','reason'],'additionalProperties':False}},
        'analysis_summary':{'type':'string'},'market_regime_summary':{'type':'string'},'residual_risks':{'type':'array','items':{'type':'string'}}},
      'required':['verdict','selected','ranking','analysis_summary','market_regime_summary','residual_risks'],'additionalProperties':False}

PROMPT='''You are the ATE FX Cloud Portfolio Arbiter for a DEMO account. You never send orders and never create a strategy. Choose only from candidate_id values supplied in the request, or choose NO_TRADE/DEFER. Compare candidates across pairs using local model evidence and threshold margin, market regime, multi-timeframe OHLC price action, spread, news state, portfolio concentration and existing positions. Prefer NO_TRADE when evidence is weak or contradictory. Never invent missing data. ATE FX risk/news/sizing/execution/reconcile remain final authority.'''

def call(req_obj: dict[str, Any]) -> dict[str, Any]:
    key=os.environ.get('OPENAI_API_KEY','')
    if not key: raise RuntimeError('OPENAI_API_KEY secret is missing')
    ids=[str(x.get('candidate_id')) for x in req_obj.get('candidates',[]) if isinstance(x,dict) and x.get('candidate_id')]
    body={'model':MODEL,'input':[{'role':'system','content':PROMPT},{'role':'user','content':'Analyze the exact ATE FX request and return a portfolio decision.\n'+json.dumps(req_obj,ensure_ascii=False,separators=(',',':'))}], 'text':{'format':{'type':'json_schema','name':'ate_fx_portfolio_decision','strict':True,'schema':schema(ids)}}}
    r=urllib.request.Request('https://api.openai.com/v1/responses',data=json.dumps(body,ensure_ascii=False).encode(),method='POST',headers={'Authorization':f'Bearer {key}','Content-Type':'application/json','User-Agent':'ATE-FX-GitHub-Relay/1.0'})
    try:
        with urllib.request.urlopen(r,timeout=120) as resp: raw=json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f'OpenAI HTTP {exc.code}: '+exc.read().decode(errors='replace')[:1500]) from exc
    text=''
    for item in raw.get('output',[]) if isinstance(raw,dict) else []:
        if not isinstance(item,dict): continue
        for part in item.get('content',[]) if isinstance(item.get('content',[]),list) else []:
            if isinstance(part,dict) and part.get('type') in {'output_text','text'} and isinstance(part.get('text'),str): text+=part['text']
    if not text and isinstance(raw,dict) and isinstance(raw.get('output_text'),str): text=raw['output_text']
    if not text: raise RuntimeError('OpenAI output text missing')
    obj=json.loads(text)
    if not isinstance(obj,dict): raise RuntimeError('OpenAI decision is not an object')
    return obj

def main() -> None:
    req_obj=json.loads(REQUEST.read_text(encoding='utf-8-sig'))
    if req_obj.get('schema_version')!='ate_fx_cloud.portfolio_request.v1': raise RuntimeError('request schema invalid')
    ids={str(x.get('candidate_id')) for x in req_obj.get('candidates',[]) if isinstance(x,dict)}
    if not ids:
        out={'verdict':'NO_TRADE','selected':{'candidate_id':'NONE','reason':'No local candidates'},'ranking':[],'analysis_summary':'No locally valid BUY/SELL candidates are available.','market_regime_summary':'No candidate set','residual_risks':[]}
    else:
        out=call(req_obj)
    sel=out.get('selected',{}) if isinstance(out.get('selected',{}),dict) else {}
    sid=str(sel.get('candidate_id') or 'NONE')
    if sid!='NONE' and sid not in ids: raise RuntimeError('unknown selected candidate')
    ranking=[]; seen=set()
    for row in out.get('ranking',[]) if isinstance(out.get('ranking',[]),list) else []:
        if not isinstance(row,dict): continue
        cid=str(row.get('candidate_id') or '')
        if cid in ids and cid not in seen:
            seen.add(cid); ranking.append(row)
    verdict=str(out.get('verdict') or 'DEFER').upper()
    if verdict=='SELECT' and sid=='NONE': verdict='DEFER'
    decision={
      'schema_version':'ate_fx_cloud.portfolio_decision.v1','created_utc':now(),'ttl_seconds':TTL,
      'request_id':req_obj.get('request_id'),'snapshot_hash':req_obj.get('snapshot_hash'),
      'decision_id':stable({'request_id':req_obj.get('request_id'),'model':MODEL,'output':out})[:32],
      'model':MODEL,'verdict':verdict,'selected':{'candidate_id':sid,'reason':str(sel.get('reason') or '')},
      'ranking':ranking,'analysis_summary':str(out.get('analysis_summary') or ''),'market_regime_summary':str(out.get('market_regime_summary') or ''),
      'residual_risks':[str(x) for x in out.get('residual_risks',[])[:12]] if isinstance(out.get('residual_risks',[]),list) else [],
      'safety':{'direct_trading_allowed':False,'selection_must_match_local_candidate':True,'ate_fx_final_risk_authority':True}}
    DECISION.parent.mkdir(parents=True,exist_ok=True)
    DECISION.write_text(json.dumps(decision,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'request_id':decision['request_id'],'decision_id':decision['decision_id'],'verdict':decision['verdict'],'selected':sid},ensure_ascii=False))

if __name__=='__main__': main()
