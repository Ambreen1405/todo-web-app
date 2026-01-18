# DOMAIN & LOGIC SKILLS

## TaskDomainModelingSkill
**Purpose:** Define the meaning and structure of "Task".
**What it does:**
- Defines task fields (title, completed, user_id, timestamps)
- Sets defaults and invariants for tasks
- Documents domain assumptions and rules

**What it does NOT do:**
- Write implementation code
- Execute tasks

## CRUDBehaviorDefinitionSkill
**Purpose:** Make CRUD behavior explicit and rule-driven.
**What it does:**
- Defines Create, Read, Update, Delete rules for tasks
- Enforces ownership and access checks
- Defines forbidden actions

**What it does NOT do:**
- Implement the CRUD operations
- Write code

## BusinessRuleFormalizationSkill
**Purpose:** Convert implicit business logic into formal, enforceable rules.
**What it does:**
- Defines backend-enforceable rules
- Prevents invalid task or system states
- Handles rule priority and conflict resolution

**What it does NOT do:**
- Execute the rules
- Write code