from __future__ import annotations
import argparse
import json
from survivorship_followup.service import run_fixture

def main() -> int:
    parser=argparse.ArgumentParser(description='癌症患者全周期随访采集调度器')
    parser.add_argument("fixture")
    parser.add_argument("--db",default="/tmp/survivorship_followup.sqlite3")
    args=parser.parse_args()
    result=run_fixture(args.fixture,args.db)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))
    return 0

if __name__ == "__main__": raise SystemExit(main())
