from celline.handler.base import BaseDBHandler


class GEOHandler(BaseDBHandler):
    def project_name(self) -> str:
        return "GSE"

    def sample_name(self) -> str:
        return "GSM"

    def onadd(self):
        return super().onadd()
