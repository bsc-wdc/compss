from provenance.models import Parameter


class Task:
    def __init__(self, tid: str, signature: str, method_name: str, file_name: str, params: list[Parameter]):
        self.tid = tid
        self.signature = signature
        self.method = method_name
        self.file = file_name
        self.params = params
