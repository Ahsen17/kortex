from abc import ABCMeta, abstractmethod
from typing import Any

from agno.agent import Agent
from agno.models.message import Message
from agno.team import Team
from agno.tools import Toolkit
from agno.workflow import Step, StepInput, StepOutput, Workflow, WorkflowAgent
from pydantic import BaseModel

from ..base import BaseSchema
from .models import AgentChat

__all__ = (
    "AgentExecutor",
    "CallableExecutor",
    "StandardWorkflow",
    "TeamExecutor",
)


def _convert(output: Any, output_schema: BaseSchema) -> BaseSchema | None:
    """Convert the output to the output schema."""

    match output:
        case None:
            return None
        case str():
            return output_schema.from_json(output)
        case dict():
            return output_schema.from_dict(output)
        case BaseModel():
            return output_schema.from_json(output.model_dump_json())
        case _:
            raise ValueError("Unsupported output type.")


class BaseExecutor(BaseSchema):
    """Base class for executors."""

    id: str
    name: str
    description: str


class RoleExecutor(BaseExecutor):
    """Base class for role executors."""

    role: str
    instructions: str


class CallableExecutor(BaseExecutor, metaclass=ABCMeta):
    """Base class for callable executors."""

    @abstractmethod
    def execute(self, step_input: StepInput) -> StepOutput:
        """Execute the step."""

        raise NotImplementedError()

    def to_step(self) -> Step:
        """Convert the executor to a workflow step."""

        return Step(
            step_id=self.id,
            name=self.name,
            description=self.description,
            executor=self.execute,
        )


class AgentExecutor(RoleExecutor, metaclass=ABCMeta):
    """Base class for agent executors."""

    model: AgentChat

    _tools: list[Toolkit] = []

    def add_tool(self, *tool: Toolkit) -> None:
        """Add tools to the agent."""

        self._tools.extend(tool)

    def to_agent(self) -> Agent:
        """Convert the executor to an agent."""

        return Agent(
            id=self.id,
            name=self.name,
            role=self.role,
            description=self.description,
            model=self.model,
            tools=self._tools,
            instructions=self.instructions,
        )

    def to_step(self) -> Step:
        """Convert the executor to a workflow step."""

        return Step(
            step_id=self.id,
            name=self.name,
            description=self.description,
            agent=self.to_agent(),
        )

    async def arun(
        self,
        input: str | list[Message],  # noqa: A002
        output_schema: BaseSchema,
    ) -> Any:
        """Run the agent."""

        output = await self.to_agent().arun(
            input=input,
            output_schema=output_schema,
        )

        return _convert(output, output_schema)


class TeamExecutor(RoleExecutor, metaclass=ABCMeta):
    """Base class for team executors."""

    _members: list[Agent | Team] = []

    def add_member(self, *member: Agent | Team) -> None:
        """Add sequential members to the team."""

        self._members.extend(member)

    def to_team(self) -> Team:
        """Convert the executor to a team."""

        return Team(
            id=self.id,
            name=self.name,
            role=self.role,
            description=self.description,
            instructions=self.instructions,
            members=self._members,
        )

    def to_step(self) -> Step:
        """Convert the executor to a workflow step."""

        return Step(
            step_id=self.id,
            name=self.name,
            description=self.description,
            team=self.to_team(),
        )

    async def arun(
        self,
        input: str | list[Message],  # noqa: A002
        output_schema: BaseSchema,
    ) -> Any:
        """Run the team flow."""

        output = await self.to_team().arun(
            input=input,
            output_schema=output_schema,
        )

        return _convert(output, output_schema)


type Executor = CallableExecutor | AgentExecutor | TeamExecutor


class StandardWorkflow:
    """Base class for steps workflow."""

    def __init__(
        self,
        id: str,  # noqa: A002
        name: str,
        agent: WorkflowAgent,
        description: str | None = None,
        stream: bool | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.id = id
        self.name = name
        self.description = description
        self.agent = agent
        self.stream = stream
        self.metadata = metadata

        self._workflow = self.initialize()
        self._steps = []

    def initialize(self) -> Workflow:
        """Initialize the workflow."""

        return Workflow(
            id=self.id,
            name=self.name,
            description=self.description,
            steps=self._steps,  # pyright: ignore[reportArgumentType]
            agent=self.agent,
            stream=self.stream,
            metadata=self.metadata,
        )

    def add_executor(self, *executor: Executor) -> None:
        """Add sequential executors to the workflow."""

        self._steps.extend([_e.to_step() for _e in executor])

    async def arun(
        self,
        input: str | list[Message],  # noqa: A002
        output_schema: BaseSchema,
    ) -> BaseSchema | None:
        """Run the workflow."""

        output = await self._workflow.arun(input=input)

        return _convert(output, output_schema)
