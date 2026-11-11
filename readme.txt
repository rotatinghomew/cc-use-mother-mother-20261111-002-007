测试：python3 -m unittest discover -s tests/journey_followup -p 'test_*.py'
构建：python3 -m compileall -q survivorship_followup
验收：python3 -m survivorship_followup.app.followup_cli fixtures/demo.json --db /tmp/survivorship_followup-acceptance.sqlite3
