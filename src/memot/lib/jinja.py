from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from ..base import BaseSchema
from ..base.abc import PathOrStr

__all__ = ("Jinja2Encoder",)


class Jinja2Encoder[T: BaseSchema]:
    """Encoder for Jinja2 templates."""

    def __init__(self, template_path: PathOrStr) -> None:
        """Initialize the Jinja2 encoder.

        Args:
            template_path: The path to the templates.
        """

        self.env = Environment(
            loader=FileSystemLoader(template_path),
            autoescape=select_autoescape(["jinja"]),
        )

    def render(self, template_name: str, **kwargs: T | Any) -> str:
        """Render a Jinja2 template with the given context.

        Args:
            template_name: The name of the template to render.
                Like `template.jinja`, the suffix is not required.
            **kwargs: The context to pass to the template.
        """

        template = self.env.get_template(f"{template_name}.jinja")

        return template.render(
            **{
                k: v.to_dict()
                if isinstance(
                    v,
                    BaseSchema,
                )
                else v
                for k, v in kwargs.items()
            }
        )
