# BACKEND SKILLS

## FastAPIRestSkill
**Purpose:** Define backend API behavior.
**What it does:**
- Sets up REST endpoints
- Manages dependency injection
- Uses middleware as needed

**What it does NOT do:**
- Handle frontend logic
- Implement UI

## RequestResponseSchemaSkill
**Purpose:** Define API contracts.
**What it does:**
- Creates Pydantic request models
- Handles response serialization
- Defines validation rules

**What it does NOT do:**
- Implement business logic
- Handle frontend behavior

## JWTVerificationSkill
**Purpose:** Enforce backend authentication.
**What it does:**
- Validates JWT tokens
- Extracts user information
- Blocks unauthorized access

**What it does NOT do:**
- Store tokens on client
- Handle frontend auth

## UserIsolationSkill
**Purpose:** Enforce data security per user.
**What it does:**
- Filters data by user_id
- Prevents cross-user access

**What it does NOT do:**
- Manage frontend sessions
- Write UI logic