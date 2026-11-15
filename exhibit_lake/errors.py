"""传输层与领域服务共用的稳定业务错误。"""

class LakeError(Exception):
    code = "lake_error"
    status = 400

    def __init__(self, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.details = details or {}


class InvalidInput(LakeError):
    code = "invalid_input"


class NotFound(LakeError):
    code = "not_found"
    status = 404


class Conflict(LakeError):
    code = "conflict"
    status = 409


class StateError(Conflict):
    code = "invalid_state"


class IntegrityFailure(LakeError):
    code = "integrity_failure"
    status = 422
