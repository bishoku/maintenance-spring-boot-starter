#!/usr/bin/env python3
"""Compile and test library source against each BOM; independent from same-JAR E2E."""
import argparse, pathlib, subprocess, datetime, json, os
root=pathlib.Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--label',required=True);p.add_argument('--java-home');p.add_argument('--verify',action='store_true');p.add_argument('--versions',nargs='+',default=['3.5.9','4.0.2']);a=p.parse_args()
if not all(c.isalnum() or c in '-_.' for c in a.label): p.error('invalid label')
if a.java_home:
    os.environ['JAVA_HOME']=a.java_home
    os.environ['PATH']=a.java_home+'/bin:'+os.environ['PATH']
out=root/'benchmark/reports'/a.label;out.mkdir(parents=True,exist_ok=False)
results=[]
for v in a.versions:
    with (out/(v+'.log')).open('w') as log:
        code=subprocess.run(['mvn','-B',f'-Dspring-boot.version={v}','clean','verify' if a.verify else 'test'],cwd=root,stdout=log,stderr=subprocess.STDOUT,timeout=600).returncode
    import xml.etree.ElementTree as ET
    totals=dict(tests=0,failures=0,errors=0,skipped=0)
    for path in (root/'target/surefire-reports').glob('TEST-*.xml'):
        suite=ET.parse(path).getroot()
        for key in totals: totals[key]+=int(suite.get(key,0))
    results.append(dict(version=v,exit_code=code,**totals));print(results[-1],flush=True)
(out/'results.json').write_text(json.dumps(dict(timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),java=subprocess.run(['java','-version'],capture_output=True,text=True).stderr,results=results),indent=2))
raise SystemExit(1 if any(r['exit_code'] for r in results) else 0)
