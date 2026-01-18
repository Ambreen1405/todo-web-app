# Data Model: Todo Application Phase 2

## Entity: User

**Description**: Represents a registered user of the system with authentication credentials and profile information.

**Fields**:
- `id`: Integer, Primary Key, Auto-increment
- `email`: String(255), Unique, Required - User's email address for login
- `hashed_password`: String, Required - Securely hashed password using bcrypt
- `name`: String(255), Required - User's display name
- `created_at`: DateTime, Required, Default: now() - Account creation timestamp
- `updated_at`: DateTime, Required, Default: now() - Last modification timestamp
- `is_active`: Boolean, Default: True - Whether the account is active

**Relationships**:
- One-to-Many: User has many Tasks (via user_id foreign key)

**Validation Rules**:
- Email must be valid email format
- Email must be unique across all users
- Name must be 1-255 characters
- Password must be at least 8 characters when setting (before hashing)

## Entity: Task

**Description**: Represents a user's task with title, description, completion status, and metadata.

**Fields**:
- `id`: Integer, Primary Key, Auto-increment
- `title`: String(255), Required - Task name/title (1-255 characters)
- `description`: String(1000), Optional - Detailed task description (max 1000 characters)
- `completed`: Boolean, Default: False - Task completion status
- `priority`: String(Enum: 'low', 'medium', 'high'), Default: 'medium' - Task importance level
- `due_date`: DateTime, Optional - Deadline for task completion
- `created_at`: DateTime, Required, Default: now() - Task creation timestamp
- `updated_at`: DateTime, Required, Default: now() - Last modification timestamp
- `user_id`: Integer, Foreign Key to User(id), Required - Owner of the task

**Relationships**:
- Many-to-One: Task belongs to one User (via user_id foreign key)

**Validation Rules**:
- Title must be 1-255 characters
- Priority must be one of: 'low', 'medium', 'high'
- Due date (if provided) must be a valid future or past date
- User_id must reference an existing active user

## State Transitions

### Task State Transitions
- **Created**: When a new task is added to the system
- **Updated**: When any field (except system-generated) is modified
- **Completed**: When the completed field changes from False to True
- **Reopened**: When the completed field changes from True to False
- **Deleted**: When the task is permanently removed

### User State Transitions
- **Registered**: When a new user account is created
- **Activated**: When account verification is completed (if applicable)
- **Deactivated**: When account is disabled by admin or user request
- **Deleted**: When account is permanently removed

## Indexes

### Required Indexes
- `users.email`: Unique index for efficient login lookup
- `tasks.user_id`: Index for efficient user-specific task retrieval
- `tasks.created_at`: Index for chronological task ordering
- `tasks.completed`: Index for filtering completed vs incomplete tasks

## Constraints

### Database-Level Constraints
- `users.email_unique`: UNIQUE(email) - Prevents duplicate email registrations
- `tasks.fk_user_id`: FOREIGN KEY(user_id) REFERENCES users(id) - Maintains referential integrity
- `tasks.priority_check`: CHECK(priority IN ('low', 'medium', 'high')) - Ensures valid priority values
- `tasks.title_not_empty`: CHECK(LENGTH(TRIM(title)) > 0) - Prevents empty titles

### Application-Level Constraints
- Users can only access their own tasks (enforced by application logic)
- Tasks cannot be created without a valid authenticated user
- System-generated fields (created_at, updated_at) cannot be modified directly by users