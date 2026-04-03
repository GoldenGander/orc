Core Requirements
1. Execution Model
A single on-prem Azure DevOps agent initiates all builds.
Build orchestration (ordering, parallelism, execution) is handled entirely by a custom Python script, not the agent.

2. Supported Build Types
Build execution is container-based — the orchestrator is agnostic to what runs inside the container.
Each job declares a Docker image and an optional command.
If no command is provided, the image's default entrypoint is used.
New build types require no changes to orchestration logic — only a new image.

3. Dependency Handling
Builds are defined as a dependency graph.
The system must:
enforce dependency order
allow parallel execution of independent jobs

4. Parallel Execution Control
Parallelism is controlled by the orchestration layer.
The system must:
support configurable concurrency limits
prevent resource overcommit via slot-based resource weights
allow reduced concurrency for high-resource jobs by declaring higher slot costs

5. Environment Isolation
Each build job runs in an isolated Docker container.
Toolchains and dependencies are encapsulated in the image and do not conflict across jobs.

6. Resource Awareness
Each job declares relative resource weight (cpu_slots, memory_slots).
The scheduler must respect total slot budgets when dispatching jobs.
Slot units are abstract — they express relative cost, not raw CPU cores or RAM bytes.

7. Artifacts and Outputs
Each job defines its output artifacts via glob patterns.
All artifacts must be collected into a unified output directory after the run.

8. Failure Handling
The system must:
detect and report per-job failures
return a single success/failure result to the pipeline
Failure policy (fail-fast vs continue) must be configurable.

9. Logging
Each job must produce isolated logs.
Logs must be attributable to specific jobs.

10. Azure DevOps Integration
The orchestration script must integrate with the existing pipeline.
The pipeline must:
trigger the build
receive final status
publish artifacts

11. Configuration-Driven Design
Jobs must be defined via configuration (not hardcoded).
Adding or modifying jobs must not require changes to orchestration logic.
