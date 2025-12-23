"""Complete usage examples for the parallel task framework.

This file demonstrates all the key features of the parallel task framework:
- Task definition with decorators
- Task submission and result retrieval
- Multi-queue processing
- Scheduling modes (immediate, delay, cron)
- Worker pool with elastic scaling
- Multi-process execution
- FastAPI integration with health checks

To run these examples, you need:
1. Valkey server running at localhost:6379
2. Optional packages: aiohttp, setproctitle, psutil, croniter
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from fastapi import FastAPI

from kortex.worker.broker.plugin.valkey import AsyncValkeyBroker
from kortex.worker.parallel import (
    HealthCheckService,
    ParallelBroker,
    ProcessConfig,
    ProcessManager,
    Scheduler,
    TaskStoreImpl,
    WorkerConfig,
    WorkerPool,
    create_health_router,
    lifespan,
    task,
)

if TYPE_CHECKING:
    from uuid import UUID

# ============================================================================
# 1. BASIC TASK DEFINITION
# ============================================================================


@task(queue="default", retry=3)
async def send_email(to: str, subject: str, body: str) -> dict[str, Any]:
    """Send an email task.

    This task will be executed by a worker. If it fails, it will be retried
    up to 3 times with exponential backoff.

    Args:
        to: Recipient email address
        subject: Email subject
        body: Email body

    Returns:
        Task result dictionary
    """
    await asyncio.sleep(0.1)
    return {"status": "sent", "to": to, "subject": subject}


@task(queue="high_priority", retry=5, priority=10)
async def process_payment(user_id: int, amount: float) -> dict[str, Any]:
    """Process a payment with high priority.

    This task is in the 'high_priority' queue and has higher priority.

    Args:
        user_id: User identifier
        amount: Payment amount

    Returns:
        Payment result
    """
    await asyncio.sleep(0.2)
    return {"status": "completed", "user_id": user_id, "amount": amount}


@task(queue="background", retry=2, ttl=3600)
async def generate_report(report_id: str) -> dict[str, Any]:
    """Generate a report task with TTL.

    If not processed within 1 hour, the task will expire.

    Args:
        report_id: Report identifier

    Returns:
        Report generation result
    """
    await asyncio.sleep(0.5)
    return {"status": "generated", "report_id": report_id}


# ============================================================================
# 2. TASK SUBMISSION AND RESULT RETRIEVAL
# ============================================================================


async def basic_usage_example() -> None:
    """Demonstrate basic task submission and result retrieval.

    Usage:
        # Submit task
        task_id = await send_email.delay(
            to="user@example.com",
            subject="Welcome!",
            body="Thank you for joining"
        )

        # Check status
        status = await send_email.status(task_id)

        # Get result (blocks until available)
        result = await send_email.result(task_id)

        # Revoke task
        revoked = await send_email.revoke(task_id)
    """
    # Submit tasks for execution
    task_id_1 = await send_email.delay(to="user@example.com", subject="Welcome!", body="Thank you for joining")

    await process_payment.delay(user_id=123, amount=99.99)

    # Wait for processing (in real usage, this happens in background)
    await asyncio.sleep(1)

    # Check task status
    await send_email.status(task_id_1)

    # Get task result (blocks until result is available)
    await send_email.result(task_id_1)

    # Revoke a task (cancel if not yet processed)
    task_id_3 = await generate_report.delay(report_id="report_001")
    await generate_report.revoke(task_id_3)


# ============================================================================
# 3. WORKER POOL WITH ELASTIC SCALING
# ============================================================================


async def worker_pool_example() -> None:
    """Demonstrate worker pool with elastic scaling.

    The worker pool manages multiple workers across queues with
    automatic scaling based on queue load.
    """
    # Create underlying broker for worker pool
    underlying_broker: AsyncValkeyBroker = AsyncValkeyBroker(host="valkey", port=6379)

    # Create worker pool
    pool = WorkerPool(
        broker=underlying_broker,
        base_workers=2,  # Always keep 2 workers running
        max_workers=5,  # Scale up to 5 workers under load
        queues=["default", "high_priority", "background"],
        auto_scale=True,  # Enable auto-scaling
        config=WorkerConfig(
            queue_timeout=5,
            task_timeout=300,
            concurrency=1,
            heartbeat_interval=30,
        ),
    )

    # Connect broker and start workers
    await underlying_broker.connect()
    await pool.start()

    # Submit some tasks (using the decorator which uses ParallelBroker internally)
    for i in range(10):
        await send_email.delay(to=f"user{i}@example.com", subject=f"Message {i}", body=f"Content {i}")

    # Wait for processing
    await asyncio.sleep(2)

    # Check pool stats
    pool.get_stats()

    # Stop pool
    await pool.stop()
    await underlying_broker.disconnect()


# ============================================================================
# 4. SCHEDULER - IMMEDIATE, DELAY, CRON
# ============================================================================


async def scheduler_example() -> None:
    """Demonstrate different scheduling modes."""
    # Create underlying broker
    underlying_broker: AsyncValkeyBroker = AsyncValkeyBroker(host="valkey", port=6379)
    await underlying_broker.connect()

    scheduler = Scheduler(underlying_broker)

    # Mode 1: Immediate execution
    await scheduler.schedule(
        task_name="send_email",
        payload={"to": "immediate@example.com", "subject": "Immediate Task", "body": "Executed immediately"},
        queue="default",
        schedule="immediate",
    )

    # Mode 2: Delayed execution (execute after 5 seconds)
    await scheduler.schedule(
        task_name="send_email",
        payload={"to": "delayed@example.com", "subject": "Delayed Task", "body": "Executed after 5 seconds"},
        queue="default",
        schedule="delay",
        delay_seconds=5,
    )

    # Mode 3: Cron-based scheduling
    # Execute every 5 minutes: "*/5 * * * *"
    await scheduler.schedule(
        task_name="generate_report",
        payload={"report_id": "daily_report"},
        queue="background",
        schedule="cron",
        cron="*/5 * * * *",  # Every 5 minutes
    )

    # Start scheduler to process delayed/cron tasks
    await scheduler.start()

    # Wait to see delayed task execute
    await asyncio.sleep(6)

    # Stop scheduler
    await scheduler.stop()
    await underlying_broker.disconnect()


# ============================================================================
# 5. MULTI-PROCESS EXECUTION
# ============================================================================


def multi_process_example() -> None:
    """Demonstrate multi-process worker execution.

    Note: This spawns actual processes. Use with caution in production.
    """
    # Process configuration
    config = ProcessConfig(
        graceful_timeout=30,  # Wait 30s for graceful shutdown
    )

    # Create process manager
    manager = ProcessManager(
        num_processes=3,  # Spawn 3 worker processes
        broker_config={"host": "valkey", "port": 6379},
        queues=["default", "high_priority"],
        config=config,
    )

    # Start all worker processes
    manager.start()

    # Check process status
    manager.get_status()

    # Get all PIDs
    manager.get_pids()

    # Check if running
    manager.is_running()

    # Stop all processes (graceful)
    manager.stop()


# ============================================================================
# 6. FASTAPI INTEGRATION WITH HEALTH CHECKS
# ============================================================================


def fastapi_integration_example() -> FastAPI:
    """Demonstrate FastAPI integration with health checks.

    Returns:
        Configured FastAPI application
    """
    # Create health service
    health_service = HealthCheckService()

    # Create underlying broker
    underlying_broker: AsyncValkeyBroker = AsyncValkeyBroker(host="valkey", port=6379)

    # Create worker pool
    worker_pool = WorkerPool(
        broker=underlying_broker,
        base_workers=2,
        max_workers=5,
        queues=["default"],
        auto_scale=True,
    )

    # Create ParallelBroker for task management
    parallel_broker = ParallelBroker(
        underlying_broker=underlying_broker,
        task_store=TaskStoreImpl(),
    )

    # Create lifespan function with bound parameters
    async def app_lifespan(app: FastAPI) -> Any:
        return lifespan(
            app,
            broker=parallel_broker,
            worker_pool=worker_pool,
            process_manager=None,
            health_service=health_service,
            start_workers=True,
            start_processes=False,
        )

    # Create FastAPI app with lifespan
    app = FastAPI(lifespan=app_lifespan)  # type: ignore

    # Create and mount health check router
    health_router = create_health_router(health_service)
    app.include_router(health_router, prefix="/api/health")

    # Add application routes
    @app.post("/tasks/email")
    async def send_email_endpoint(to: str, subject: str, body: str) -> dict[str, Any]:
        """Endpoint to submit email task."""
        task_id = await send_email.delay(to=to, subject=subject, body=body)
        return {"task_id": task_id, "status": "queued"}

    @app.get("/tasks/{task_id}")
    async def get_task_status(task_id: UUID) -> dict[str, Any]:
        """Get task status."""
        status = await send_email.status(task_id)
        return {"task_id": task_id, "status": status}

    return app


# ============================================================================
# 7. COMPLETE WORKING EXAMPLE
# ============================================================================


async def complete_example() -> None:
    """Complete working example combining all features."""
    # 1. Setup underlying broker
    underlying_broker: AsyncValkeyBroker = AsyncValkeyBroker(host="valkey", port=6379)
    await underlying_broker.connect()

    # 2. Setup worker pool
    pool = WorkerPool(
        broker=underlying_broker,
        base_workers=2,
        max_workers=4,
        queues=["default", "high_priority"],
        auto_scale=True,
    )
    await pool.start()

    # 3. Setup scheduler
    scheduler = Scheduler(underlying_broker)
    await scheduler.start()

    # 4. Submit various tasks
    # Regular tasks
    await send_email.delay("user1@example.com", "Hello", "Message 1")
    await process_payment.delay(user_id=100, amount=50.0)

    # Delayed task
    await scheduler.schedule(
        task_name="send_email",
        payload={"to": "delayed@example.com", "subject": "Delayed", "body": "Later"},
        schedule="delay",
        delay_seconds=3,
    )

    # 6. Monitor progress
    await asyncio.sleep(1)

    # Check pool stats
    pool.get_stats()

    # Check task results
    await send_email.result(uuid4())

    # 7. Cleanup
    await scheduler.stop()
    await pool.stop()
    await underlying_broker.disconnect()


# ============================================================================
# MAIN
# ============================================================================


async def main() -> None:
    """Run all examples.

    Note: These examples require a Valkey server at localhost:6379.
    To run specific examples, call them directly.
    """

    await basic_usage_example()


if __name__ == "__main__":
    asyncio.run(main())
