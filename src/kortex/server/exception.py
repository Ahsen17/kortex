__all__ = ("ApplicationError",)


class ApplicationError(Exception):
    """Base class for all application-specific errors."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
