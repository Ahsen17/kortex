from pathlib import Path
from typing import Any, overload

from jinja2 import Environment, FileSystemLoader, select_autoescape
from msgspec import Struct, to_builtins
from pydantic import BaseModel

__all__ = ("Jinja2Renderer",)


type PathOrStr = str | Path


class Jinja2Renderer:
    """Render for Jinja2 templates."""

    def __init__(self, template_path: PathOrStr) -> None:
        """Initialize the Jinja2 encoder.

        Args:
            template_path: The path to the templates.
        """

        self.env = Environment(
            loader=FileSystemLoader(template_path),
            autoescape=select_autoescape(["jinja"]),
        )

    @overload
    def render(self, template_name: str, **kwargs: Struct) -> str: ...

    @overload
    def render(self, template_name: str, **kwargs: BaseModel) -> str: ...

    @overload
    def render(self, template_name: str, **kwargs: Any) -> str: ...

    def render(
        self,
        template_name: str,
        **kwargs: Struct | BaseModel | Any,
    ) -> str:
        """Render a Jinja2 template with the given context.

        Args:
            template_name: The name of the template to render.
                Like `template.jinja`, the suffix is not required.
            **kwargs: The context to pass to the template.
        """

        template = self.env.get_template(f"{template_name}.jinja")

        for k, v in kwargs.items():
            match v:
                case Struct():
                    kwargs[k] = to_builtins(v)
                case BaseModel():
                    kwargs[k] = v.model_dump()
                case _:
                    kwargs[k] = v

        return template.render(**kwargs)
