Core Requirements
1. Execution Model
A single on-prem Azure DevOps agent initiates all builds.
Build orchestration (ordering, parallelism, execution) is handled entirely by a custom script, not the agent.
2. Supported Build Types
Must support:
Qt for WebAssembly builds (Emscripten-based)
Visual Studio / MSBuild projects
Each project declares its build type and required environment.
3. Dependency Handling
Builds are defined as a dependency graph.
The system must:
enforce dependency order
allow parallel execution of independent projects
4. Parallel Execution Control
Parallelism is controlled by the orchestration layer.
The system must:
support configurable concurrency limits
prevent resource overcommit (especially RAM)
allow reduced concurrency for high-memory jobs (e.g., Wasm optimization)
5. Environment Isolation
Build jobs must run in isolated environments (containerized or equivalent).
Toolchains (Qt, Emscripten, MSBuild, SDKs) must not conflict across jobs.
6. Resource Awareness
Each job must declare resource expectations (at least relative weight or class).
Scheduler must respect CPU and memory constraints when dispatching jobs.
7. Workspace Isolation
Each job runs in an isolated working directory.
Builds must not interfere with each other’s intermediate or output files.
8. Artifacts and Outputs
Each project defines its output artifacts.
All artifacts must be collected into a unified output structure.
9. Failure Handling
The system must:
detect and report per-job failures
return a single success/failure result to the pipeline
Failure policy (fail-fast vs continue) must be configurable.
10. Logging
Each job must produce isolated logs.
Logs must be attributable to specific projects and build steps.
11. Azure DevOps Integration
The orchestration script must integrate with the existing pipeline.
The pipeline must:
trigger the build
receive final status
publish artifacts
12. Configuration-Driven Design
Projects must be defined via configuration (not hardcoded).
Adding or modifying projects must not require changes to orchestration logic.
