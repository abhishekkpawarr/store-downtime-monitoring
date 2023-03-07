from fastapi import APIRouter

from report import report

router = APIRouter()


@router.post("/trigger_report")
async def trigger_report():
    return await report.trigger_report()


@router.get("/get_report/{report_id}")
async def get_report(report_id: str):
    return await report.get_report(report_id)
