---
id: 1
title: "Update Project Constitution"
stage: "constitution"
date_iso: "2026-01-14"
surface: "agent"
model: "claude-sonnet-4-5-20250929"
feature: "none"
branch: "main"
user: "adeel"
command: "/sp.constitution"
labels: ["constitution", "governance", "setup"]
links:
  spec: null
  ticket: null
  adr: null
  pr: null
files_yaml:
  - ".specify/memory/constitution.md"
tests_yaml: []
---

# Prompt History Record

## Original Prompt

```
You are a Specification-Driven System Architect AI responsible for designing and guiding Phase 2 of a Todo Application.

This project starts directly from Phase 2.
Phase 1 (CLI application) is intentionally skipped, and no Phase 1 code is allowed.

However, all logical concepts normally introduced in Phase 1 must be explicitly defined inside Phase 2 specifications.

PHASE 2 OBJECTIVE

Build a secure, production-grade full-stack web application with:

Frontend: Next.js (App Router) + Tailwind CSS
Backend: FastAPI (REST)
Database: PostgreSQL (via SQLModel, hosted on Neon)
Authentication: JWT-based auth (Better Auth or equivalent)

CONSTITUTIONAL PRINCIPLES

1. Specification-First Development
No code may be written without an approved specification.
All behavior must be explicitly described.
Implicit logic is forbidden.

2. Phase Isolation
Only Phase 2 scope is allowed.
No Phase 3+ features (AI, chat, MCP, analytics, automation).
No CLI or terminal-based application.

3. Explicit Domain Definition
The Task domain must be fully defined in Phase 2 specs:
Fields
Defaults
Valid/invalid states
CRUD behavior
Domain logic must not be assumed.

4. Strong Separation of Concerns
Frontend, backend, authentication, database, and specifications must be strictly separated.
Database schemas are defined only by the database agent.
Backend consumes schemas but does not define them.
Frontend never bypasses backend rules.

5. Authentication & Security First
All task operations require a valid JWT.
Users can only access their own data.
Unauthorized access must be rejected explicitly.

6. Agent-Governed Development
All work must be performed by specialized agents with fixed responsibilities:
system-architect-agent
backend-agent
frontend-agent
auth-integration-agent
sqlmodel-database-agent
domain-specification-agent
planner-decomposition-agent
integration-agent
quality-spec-guard-agent
skills-create-agent

No agent may operate outside its defined scope.

7. Skill-Governed Capabilities
Agents may only use approved skills.
Skills must be atomic, documented, and non-overlapping.
No agent may invent a skill independently.

8. Quality as a Gate
Every phase output must pass:
Spec completeness checks
Security checks
Integration checks
The Quality & Spec Guard Agent has final authority.

HARD CONSTRAINTS

No vibe coding
No undocumented behavior
No shared mutable state across users
No assumptions without specs
No database logic inside frontend

SUCCESS CRITERIA (PHASE 2)

Phase 2 is considered complete only if:
A user can sign up and log in securely
JWT is enforced on every protected route
Users can create, read, update, and delete only their own tasks
Frontend and backend contracts are aligned
All behavior is traceable to specifications
No Phase 1 or Phase 3 logic exists

DECISION PRIORITY ORDER

1. Constitution
2. Approved Specifications
3. Phase 2 Scope Rules
4. Security & Data Isolation
5. Simplicity over cleverness

CONSTITUTIONAL RULE

If it is not specified, it does not exist.
```

## Response Summary

Updated the project constitution file (.specify/memory/constitution.md) with specific principles and constraints for the Todo Application Phase 2 project. The constitution now includes:

- 8 core principles covering specification-first development, phase isolation, explicit domain definition, separation of concerns, security-first approach, agent-governed development, skill-governed capabilities, and quality gates
- Additional constraints for technology stack requirements, security & data isolation, and code quality standards
- Development workflow guidelines for implementation process, review process, and quality gates
- Governance rules for constitutional compliance

The constitution version is set to 1.0.0, ratified on 2026-01-14, and follows the requirements specified by the user for Phase 2 of the Todo Application.

## Outcome

The project constitution has been successfully updated with all required principles and constraints for Phase 2 development. This document will serve as the foundational governance document for all subsequent development activities in the project.

## Evaluation

The constitution has been created with all required elements:
- All 8 specified principles have been incorporated
- Technology stack requirements are clearly defined
- Security and isolation requirements are specified
- Agent governance structure is documented
- Quality gates and compliance requirements are established
- The document follows the proper format and structure