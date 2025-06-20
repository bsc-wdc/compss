from provenance.models import Parameter


class Task:
    def __init__(self, tid: str, signature: str, method_name: str, file_name: str, params: list[Parameter]):
        self.tid = tid
        self.signature = signature
        self.host = ""
        self.method = method_name
        self.file = file_name
        self.params = params

    def __str__(self):
        param_str = ", ".join(str(p) for p in self.params)
        return (
            f"Task(tid='{self.tid}', signature='{self.signature}', "
            f"method='{self.method}', file='{self.file}', params=[{param_str}])"
        )

    def __repr__(self):
        return self.__str__()