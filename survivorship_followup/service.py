from __future__ import annotations
import json
import uuid
from pathlib import Path
from typing import Any, Mapping
from survivorship_followup.runtime import DomainError, ProcessingContext, SQLiteLedger, fingerprint, normalize_text
from survivorship_followup.cohorts.enrollment import build_processor as enrollment_factory
from survivorship_followup.plans.care_templates import build_processor as care_templates_factory
from survivorship_followup.scheduler.timewheel import build_processor as timewheel_factory
from survivorship_followup.channels.contact_dispatch import build_processor as contact_dispatch_factory
from survivorship_followup.responses.merge import build_processor as merge_factory
from survivorship_followup.escalation.symptom_rules import build_processor as symptom_rules_factory
from survivorship_followup.reengagement.lost_to_followup import build_processor as lost_to_followup_factory
from survivorship_followup.outcomes.survivorship import build_processor as survivorship_factory

PROCESSOR_FACTORIES = [enrollment_factory, care_templates_factory, timewheel_factory, contact_dispatch_factory, merge_factory, symptom_rules_factory, lost_to_followup_factory, survivorship_factory]

class ApplicationService:
    def __init__(self, database: str | Path):
        self.ledger=SQLiteLedger(database)
        self.processors=[factory() for factory in PROCESSOR_FACTORIES]

    def validate_input(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping): raise DomainError("record must be object")
        result=dict(record)
        result["case_id"]=normalize_text(result.get("case_id"),"case_id",96)
        if not result["case_id"]: raise DomainError("case_id is required")
        result.setdefault("subject_id",result["case_id"])
        result.setdefault("category","screening")
        result.setdefault("region","synthetic-region")
        result.setdefault("event_date","2026-11-11")
        result.setdefault("status","new")
        result.setdefault("count",1)
        result.setdefault("total",1)
        result.setdefault("attributes",{"source":"synthetic"})
        result.setdefault("tags",["offline","fixture"])
        result.setdefault("source_system","local-fixture")
        result.setdefault("revision",0)
        return result

    def execute(self, record: Mapping[str, Any], request_key: str | None=None) -> dict[str, Any]:
        normalized=self.validate_input(record)
        normalized=self.architecture_preflight(normalized)
        key=request_key or fingerprint(normalized)
        recalled=self.ledger.recalled(key)
        if recalled is not None: return recalled | {"replayed":True}
        ctx=ProcessingContext(uuid.uuid4().hex,normalized["case_id"],normalized)
        ctx.emit("received","run_started",project='survivorship_followup')
        try:
            for processor in self.architecture_plan(ctx):
                processor.process(ctx)
                self.ledger.save(ctx)
        except Exception as exc:
            self.ledger.dead_letter(ctx,ctx.state,str(exc),normalized)
            raise
        self.architecture_finalize(ctx)
        ctx.advance("completed")
        self.ledger.save(ctx)
        response={"project":'survivorship_followup',"run_id":ctx.run_id,"case_id":ctx.case_id,"state":ctx.state,
                  "revision":ctx.revision,"flags":ctx.flags,"metrics":ctx.metrics,"evidence_count":len(ctx.evidence),
                  "event_count":len(ctx.events),"payload":ctx.payload,"replayed":False}
        self.ledger.remember(key,ctx.run_id,response)
        return response

    def architecture_preflight(self, record):
        record=dict(record)
        record["timewheel"]={"buckets":{},"attempts":[],"escalations":[],"timezone":"Asia/Shanghai"}
        return record

    def architecture_plan(self, ctx):
        wheel=ctx.payload["timewheel"]
        plan=[]
        for index,processor in enumerate(self.processors):
            bucket=f"day-{index*14:03}"
            wheel["buckets"].setdefault(bucket,[]).append(processor.stage)
            plan.append(processor)
        ctx.emit("timewheel","schedule_materialized",buckets=len(wheel["buckets"]))
        return plan

    def architecture_finalize(self, ctx):
        wheel=ctx.payload["timewheel"]
        wheel["attempts"].append({"channel":"synthetic-call","result":"review" if ctx.flags else "answered"})
        if ctx.flags: wheel["escalations"].extend(ctx.flags)
        ctx.metrics["scheduled_buckets"]=len(wheel["buckets"])
        ctx.emit("survivorship","journey_updated",escalations=len(wheel["escalations"]))

    def execute_many(self, records: list[Mapping[str, Any]]) -> dict[str, Any]:
        outputs=[]; failures=[]
        for index, record in enumerate(records):
            try: outputs.append(self.execute(record,request_key=f"batch:{index}:{fingerprint(record)}"))
            except Exception as exc: failures.append({"index":index,"error":str(exc)})
        return {"accepted":outputs,"failures":failures,"accepted_count":len(outputs),"failure_count":len(failures)}

def run_fixture(fixture: str | Path, database: str | Path) -> dict[str, Any]:
    data=json.loads(Path(fixture).read_text(encoding="utf-8"))
    service=ApplicationService(database)
    if isinstance(data,list): return service.execute_many(data)
    return service.execute(data)
