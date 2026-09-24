#!/usr/bin/env python3
"""GitHub Actions worker for ATE FX Cloud Arbiter. Standard library only."""
from __future__ import annotations
import hashlib,json,os,time,urllib.request,urllib.error
from pathlib import Path

REQUEST=Path('ate_fx_cloud/requests/latest.json')
DECISION=Path('ate_fx_cloud/decisions/latest.json')
MODEL=os.environ.get('OPENAI_MODEL','gpt-5.6-sol')
TTL=max(60,min(1800,int(os.environ.get('ATE_DECISION_TTL_SECONDS','600'))))

SCHEMA={
  "type":"object","additionalProperties":False,
  "properties":{
    "action":{"type":"string","enum":["NO_TRADE","OPEN"]},
    "pair":{"type":"string","maxLength":16},
    "direction":{"type":"string","enum":["NONE","BUY","SELL"]},
    "confidence":{"type":"number","minimum":0,"maximum":1},
    "stop_atr":{"type":"number","minimum":0,"maximum":3},
    "target_atr":{"type":"number","minimum":0,"maximum":6},
    "max_hold_hours":{"type":"integer","minimum":0,"maximum":12},
    "rationale_codes":{"type":"array","maxItems":8,"items":{"type":"string","maxLength":80}},
    "risk_flags":{"type":"array","maxItems":8,"items":{"type":"string","maxLength":80}}
  },
  "required":["action","pair","direction","confidence","stop_atr","target_atr","max_hold_hours","rationale_codes","risk_flags"]
}

SYSTEM="""You are the portfolio arbiter for an automated FOREX research system running ONLY on a DEMO account.
Use ONLY the supplied market snapshot; do not invent news, prices, correlations, or facts that are absent.
Your task is selective: choose at most one new position across all pairs, or NO_TRADE.
Prioritize coherent multi-factor setups over activity. Treat confidence as a calibrated decision-strength estimate, never certainty.
Strong reasons for NO_TRADE include contradictory direction evidence, excessive spread/ATR, weak trend structure, unstable extremes,
existing exposure, or no clear edge across pairs. Never choose AUDCAD when the request says its history is untested.
For OPEN: stop_atr must be 0.75..3.0, target_atr 1.0..6.0, max_hold_hours 1..12.
Prefer reward/risk >= 1 unless the structure clearly justifies otherwise. Do not exceed request constraints.
Return only the required structured output."""

def canon(o): return json.dumps(o,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def sha(o): return hashlib.sha256(canon(o).encode()).hexdigest()
def out_text(resp):
    for item in resp.get('output',[]):
        if item.get('type')=='message':
            for c in item.get('content',[]):
                if c.get('type')=='output_text' and isinstance(c.get('text'),str): return c['text']
    raise ValueError('NO_OUTPUT_TEXT')

def call_openai(req):
    key=os.environ.get('OPENAI_API_KEY','').strip()
    if not key: raise RuntimeError('OPENAI_API_KEY_MISSING')
    body={
      'model':MODEL,
      'reasoning':{'effort':'high'},
      'input':[
        {'role':'system','content':SYSTEM},
        {'role':'user','content':'ATE FX decision snapshot JSON:\n'+canon(req)}
      ],
      'text':{'format':{'type':'json_schema','name':'ate_fx_portfolio_decision','strict':True,'schema':SCHEMA}},
      'max_output_tokens':1600,
      'store':False
    }
    r=urllib.request.Request('https://api.openai.com/v1/responses',data=json.dumps(body).encode(),method='POST',headers={
      'Authorization':'Bearer '+key,'Content-Type':'application/json','User-Agent':'ATE-FX-Cloud-Arbiter/575R4'})
    try:
      with urllib.request.urlopen(r,timeout=120) as h: resp=json.loads(h.read().decode())
    except urllib.error.HTTPError as e:
      raise RuntimeError('OPENAI_HTTP_%s:%s'%(e.code,e.read().decode(errors='replace')[-500:])) from e
    return json.loads(out_text(resp)),resp.get('id','')

def sanitize(model_out,req):
    allowed={x.get('pair') for x in req.get('market',[]) if isinstance(x,dict)}
    action=model_out.get('action');pair=str(model_out.get('pair',''));direction=model_out.get('direction')
    try:
      conf=float(model_out.get('confidence',0));stop=float(model_out.get('stop_atr',0));target=float(model_out.get('target_atr',0));hold=int(model_out.get('max_hold_hours',0))
    except Exception:
      action='NO_TRADE';conf=stop=target=0;hold=0
    if action=='OPEN':
      if pair not in allowed or direction not in ('BUY','SELL') or not (0.75<=stop<=3 and 1<=target<=6 and 1<=hold<=12):
        return {'action':'NO_TRADE','pair':'','direction':'NONE','confidence':0.0,'stop_atr':0.0,'target_atr':0.0,'max_hold_hours':0,
                'rationale_codes':['WORKER_INVALID_OPEN_OUTPUT'],'risk_flags':['FAIL_CLOSED']}
    else:
      action='NO_TRADE';pair='';direction='NONE';stop=target=0.0;hold=0
    return {'action':action,'pair':pair,'direction':direction,'confidence':max(0,min(1,conf)),'stop_atr':stop,'target_atr':target,'max_hold_hours':hold,
            'rationale_codes':[str(x)[:80] for x in model_out.get('rationale_codes',[])][:8],
            'risk_flags':[str(x)[:80] for x in model_out.get('risk_flags',[])][:8]}

def main():
    req=json.loads(REQUEST.read_text(encoding='utf-8'))
    if req.get('schema_version')!='ATE_FX_CLOUD_REQUEST_V1': raise SystemExit('bad request schema')
    now=int(time.time()); api_id=''; err=''
    try:
      mo,api_id=call_openai(req);choice=sanitize(mo,req)
    except Exception as e:
      err=str(e)[:300]
      choice={'action':'NO_TRADE','pair':'','direction':'NONE','confidence':0.0,'stop_atr':0.0,'target_atr':0.0,'max_hold_hours':0,
              'rationale_codes':['OPENAI_CALL_FAILED'],'risk_flags':['FAIL_CLOSED',err]}
    decision={
      'schema_version':'ATE_FX_CLOUD_DECISION_V1','request_id':req['request_id'],'snapshot_hash':req['snapshot_hash'],
      'generated_unix':now,'expires_unix':now+TTL,'model':MODEL,'openai_response_id':api_id,
      **choice
    }
    decision['decision_id']=hashlib.sha256((req['request_id']+'|'+canon(choice)+'|'+str(now)).encode()).hexdigest()[:32]
    DECISION.parent.mkdir(parents=True,exist_ok=True);DECISION.write_text(json.dumps(decision,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    hist=Path('ate_fx_cloud/decisions/history')/time.strftime('%Y-%m-%d',time.gmtime(now))/(decision['decision_id']+'.json')
    hist.parent.mkdir(parents=True,exist_ok=True);hist.write_text(DECISION.read_text(encoding='utf-8'),encoding='utf-8')
    print(json.dumps({'decision_id':decision['decision_id'],'action':decision['action'],'pair':decision['pair'],'error':err},ensure_ascii=False))
if __name__=='__main__': main()
