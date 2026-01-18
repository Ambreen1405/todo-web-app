import { apiClient } from './client';
import { User } from '../types/user';

interface LoginCredentials {
  email: string;
  password: string;
}

interface RegisterData {
  email: string;
  name: string;
  password: string;
}

interface LoginResponse {
  access_token: string;
  token_type: string;
}

export const authApi = {
  /**
   * Register a new user
   */
  async register(
  userData: RegisterData
): Promise<{ success: boolean; error?: string }> {
  try {
    const response = await apiClient.post('/auth/register', userData);

    if (response.success) {
      return { success: true };
    }

    return {
      success: false,
      error: response.error || 'Registration failed',
    };
  } catch (error: any) {
    return {
      success: false,
      error: error.message || 'Registration failed',
    };
  }
},

  /**
   * Login user and return token
   */
  async login(credentials: LoginCredentials): Promise<{ success: boolean; token?: string; error?: string }> {
    try {
      const response = await apiClient.post('/auth/login', credentials);

      if (response.success && response.data) {
        const data = response.data as LoginResponse;
        return { success: true, token: data.access_token };
      } else {
        return { success: false, error: response.error };
      }
    } catch (error: any) {
      return { success: false, error: error.message || 'Login failed' };
    }
  },

  /**
   * Logout user
   */
  async logout(): Promise<{ success: boolean; error?: string }> {
    // In a real app, you would call the logout endpoint
    // For now, we just return success and let the context handle cleanup
    return { success: true };
  },

  /**
   * Get current user info
   */
  async getCurrentUser(token: string): Promise<{ success: boolean; user?: User; error?: string }> {
    try {
      const response = await apiClient.get('/auth/me', {
        'Authorization': `Bearer ${token}`
      });

      if (response.success && response.data) {
        return { success: true, user: response.data as User };
      } else {
        return { success: false, error: response.error };
      }
    } catch (error: any) {
      return { success: false, error: error.message || 'Failed to get user info' };
    }
  }
};