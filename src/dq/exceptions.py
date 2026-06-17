"""Data quality exceptions."""


class DataQualityError(RuntimeError):
    """Raised when one or more DQ checks fail and fail-fast is enabled."""

    def __init__(self, message: str, report_path: str | None = None):
        super().__init__(message)
        self.report_path = report_path