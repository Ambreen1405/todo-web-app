import { test, expect } from '@playwright/test';

// End-to-end test for the complete task management workflow
test.describe('Task Management Workflow', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the app
    await page.goto('http://localhost:3000');
  });

  test('complete user workflow: register, login, create, update, and delete tasks', async ({ page }) => {
    // 1. Register a new user
    await page.getByRole('link', { name: 'Register' }).click();

    await page.locator('#email').fill('e2e-test@example.com');
    await page.locator('#name').fill('E2E Test User');
    
    await page.locator('#password').fill('SecurePassword123!');
    await page.locator('#confirm-password').fill('SecurePassword123!');

    await page.getByRole('button', { name: 'Register' }).click();

    // Verify registration success and redirect to dashboard
    await expect(page).toHaveURL('http://localhost:3000/dashboard');
    await expect(page.getByText('Welcome, E2E Test User')).toBeVisible();

    // 2. Create a new task
    await page.locator('#title').fill('E2E Test Task');
    await page.locator('#description').fill('This is a test task created during E2E testing');
    await page.locator('#priority').selectOption('high');
    await page.locator('#due_date').fill('2023-12-31');

    await page.getByRole('button', { name: 'Create Task' }).click();

    // Verify task was created
    await expect(page.getByText('E2E Test Task')).toBeVisible();
    await expect(page.getByText('This is a test task created during E2E testing')).toBeVisible();
    await expect(page.getByText('high')).toBeVisible();

    // 3. Update the task
    await page.getByRole('button', { name: 'Edit' }).click();

    const titleInput = page.locator('input[value="E2E Test Task"]');
    await titleInput.fill('Updated E2E Test Task');

    const descriptionInput = page.locator('textarea').nth(0); // First textarea in edit form
    await descriptionInput.fill('This is an updated test task');

    await page.getByRole('button', { name: 'Save' }).click();

    // Verify task was updated
    await expect(page.getByText('Updated E2E Test Task')).toBeVisible();
    await expect(page.getByText('This is an updated test task')).toBeVisible();

    // 4. Mark task as completed
    const taskCheckbox = page.locator('input[type="checkbox"]').first();
    await taskCheckbox.check();

    // Verify task is marked as completed (check for strikethrough or completed indicator)
    await expect(page.getByText('Updated E2E Test Task')).toHaveClass(/line-through/);

    // 5. Delete the task
    await page.getByRole('button', { name: 'Delete' }).click();

    // Confirm deletion (assuming a confirmation dialog appears)
    page.on('dialog', dialog => dialog.accept());

    // Verify task is deleted
    await expect(page.getByText('Updated E2E Test Task')).not.toBeVisible();

    // 6. Logout
    await page.getByRole('button', { name: 'Logout' }).click();

    // Verify logout success and redirect to login
    await expect(page).toHaveURL('http://localhost:3000/login');
    await expect(page.getByText('Please sign in to your account')).toBeVisible();
  });

  test('user cannot access other users\' tasks', async ({ page }) => {
    // Create and login as first user
    await page.goto('http://localhost:3000/register');
    await page.locator('#email').fill('user1@example.com');
    await page.locator('#name').fill('User 1');
    await page.locator('#password').fill('Password123!');
    await page.locator('#confirm-password').fill('Password123!');
    await page.getByRole('button', { name: 'Register' }).click();

    // Create a task for user 1
    await page.locator('#title').fill('User 1 Task');
    await page.getByRole('button', { name: 'Create Task' }).click();
    await expect(page.getByText('User 1 Task')).toBeVisible();

    // Store the task ID somehow (would need to extract from UI in real implementation)
    const user1TaskId = 'user1-task'; // Placeholder

    // Logout user 1
    await page.getByRole('button', { name: 'Logout' }).click();

    // Create and login as second user
    await page.locator('#email').fill('user2@example.com');
    await page.locator('#name').fill('User 2');
    await page.locator('#password').fill('Password123!');
    await page.locator('#confirm-password').fill('Password123!');
    await page.getByRole('button', { name: 'Register' }).click();

    // Try to access user 1's task directly via URL (this should fail)
    // await page.goto(`http://localhost:3000/tasks/${user1TaskId}`);
    // await expect(page.getByText('Access Denied')).toBeVisible();
    // Or expect a 404 or redirect to dashboard

    // Verify user 2 only sees their own tasks
    await expect(page.getByText('User 1 Task')).not.toBeVisible();

    // Create a task for user 2
    await page.locator('#title').fill('User 2 Task');
    await page.getByRole('button', { name: 'Create Task' }).click();
    await expect(page.getByText('User 2 Task')).toBeVisible();

    // Verify user 2 still doesn't see user 1's task
    await expect(page.getByText('User 1 Task')).not.toBeVisible();
  });
});

test.describe('Authentication Workflow', () => {
  test('user can register, login, and logout successfully', async ({ page }) => {
    // Navigate to register page
    await page.goto('http://localhost:3000/register');

    // Fill registration form
    await page.locator('#email').fill('auth-test@example.com');
    await page.locator('#name').fill('Auth Test User');
    await page.locator('#password').fill('SecurePassword123!');
    await page.locator('#confirm-password').fill('SecurePassword123!');

    // Submit registration
    await page.getByRole('button', { name: 'Register' }).click();

    // Verify successful registration and redirect to dashboard
    await expect(page).toHaveURL('http://localhost:3000/dashboard');
    await expect(page.getByText('Welcome, Auth Test User')).toBeVisible();

    // Verify user stays logged in after page refresh
    await page.reload();
    await expect(page.getByText('Welcome, Auth Test User')).toBeVisible();

    // Logout
    await page.getByRole('button', { name: 'Logout' }).click();

    // Verify logout success and redirect to login
    await expect(page).toHaveURL('http://localhost:3000/login');
    await expect(page.getByText('Please sign in to your account')).toBeVisible();

    // Try to access dashboard without login (should redirect)
    await page.goto('http://localhost:3000/dashboard');
    await expect(page).toHaveURL('http://localhost:3000/login');
  });

  test('login with invalid credentials shows error', async ({ page }) => {
    await page.goto('http://localhost:3000/login');

    await page.locator('#email').fill('nonexistent@example.com');
    await page.locator('#password').fill('wrongpassword');

    await page.getByRole('button', { name: 'Sign in' }).click();

    // Verify error message is displayed
    await expect(page.getByText(/invalid credentials|incorrect|failed/i)).toBeVisible();
  });
});