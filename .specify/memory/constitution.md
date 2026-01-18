<!-- SYNC IMPACT REPORT
Version change: N/A → 1.0.0
Modified principles: None (new constitution)
Added sections: All sections (new constitution)
Removed sections: None
Templates requiring updates:
- .specify/templates/plan-template.md ✅ updated
- .specify/templates/spec-template.md ✅ updated
- .specify/templates/tasks-template.md ✅ updated
- .specify/templates/commands/*.md ⚠ pending
Follow-up TODOs: None
-->
# Todo Application Phase 2 Constitution

## Core Principles

### Specification-First Development
No code may be written without an approved specification. All behavior must be explicitly described. Implicit logic is forbidden. Every feature and functionality must be fully specified before implementation begins.

### Phase Isolation
Only Phase 2 scope is allowed. No Phase 1 (CLI application) or Phase 3+ features (AI, chat, MCP, analytics, automation) are permitted. No CLI or terminal-based application functionality may be implemented.

### Explicit Domain Definition
The Task domain must be fully defined in Phase 2 specs with all fields, defaults, valid/invalid states, and CRUD behavior explicitly documented. Domain logic must not be assumed or inferred.

### Strong Separation of Concerns
Frontend, backend, authentication, database, and specifications must be strictly separated. Database schemas are defined only by the database agent. Backend consumes schemas but does not define them. Frontend never bypasses backend rules.

### Authentication & Security First
All task operations require a valid JWT. Users can only access their own data. Unauthorized access must be rejected explicitly. Security measures are paramount and non-negotiable.

### Agent-Governed Development
All work must be performed by specialized agents with fixed responsibilities: system-architect-agent, backend-agent, frontend-agent, auth-integration-agent, sqlmodel-database-agent, domain-specification-agent, planner-decomposition-agent, integration-agent, quality-spec-guard-agent, skills-create-agent. No agent may operate outside its defined scope.

### Skill-Governed Capabilities
Agents may only use approved skills. Skills must be atomic, documented, and non-overlapping. No agent may invent a skill independently.

### Quality as a Gate
Every phase output must pass spec completeness checks, security checks, and integration checks. The Quality & Spec Guard Agent has final authority over all deliverables.

## Additional Constraints

### Technology Stack Requirements
- Frontend: Next.js (App Router) + Tailwind CSS
- Backend: FastAPI (REST)
- Database: PostgreSQL (via SQLModel, hosted on Neon)
- Authentication: JWT-based auth (Better Auth or equivalent)

### Security & Data Isolation
- No shared mutable state across users
- Users can only access their own data
- All database logic must remain in the database layer, never in frontend
- JWT enforcement on every protected route

### Code Quality Standards
- No vibe coding
- No undocumented behavior
- No assumptions without specifications
- Minimal, testable changes only

## Development Workflow

### Implementation Process
1. Specification must be approved before any code is written
2. Use designated agents for specific tasks only
3. Follow the sequence: Domain Spec → Plan → Tasks → Implementation
4. All changes must be small, testable, and precisely referenced

### Review Process
- All implementations must trace back to approved specifications
- Cross-agent integration points must be validated by integration-agent
- Quality-spec-guard-agent performs final validation before acceptance

### Quality Gates
- All behavior must be traceable to specifications
- Security checks must pass for all features
- Integration tests must validate frontend-backend contracts
- Authentication must be verified on all protected routes

## Governance

All development activities must comply with this constitution. Amendments require explicit documentation, approval from project stakeholders, and a clear migration plan. All pull requests and reviews must verify constitutional compliance. The constitution supersedes all other development practices and guidelines.

**Version**: 1.0.0 | **Ratified**: 2026-01-14 | **Last Amended**: 2026-01-14