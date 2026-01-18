/**
 * Authentication utilities for token management
 */

export const TOKEN_KEY = 'token';

/**
 * Store the JWT token in local storage
 */
export const setToken = (token: string): void => {
  if (typeof window !== 'undefined') {
    localStorage.setItem(TOKEN_KEY, token);
  }
};

/**
 * Retrieve the JWT token from local storage
 */
export const getToken = (): string | null => {
  if (typeof window !== 'undefined') {
    return localStorage.getItem(TOKEN_KEY);
  }
  return null;
};

/**
 * Remove the JWT token from local storage
 */
export const removeToken = (): void => {
  if (typeof window !== 'undefined') {
    localStorage.removeItem(TOKEN_KEY);
  }
};

/**
 * Check if a token exists
 */
export const hasToken = (): boolean => {
  return !!getToken();
};

/**
 * Decode a JWT token to get its payload
 * Note: This is a simple decoder and doesn't verify the token signature
 */
export const decodeToken = (token: string): any => {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) {
      throw new Error('Invalid token format');
    }

    const payload = parts[1];
    // Add padding if needed
    const paddedPayload = payload.replace(/-/g, '+').replace(/_/g, '/');
    const decodedPayload = atob(paddedPayload);
    return JSON.parse(decodedPayload);
  } catch (error) {
    console.error('Error decoding token:', error);
    return null;
  }
};

/**
 * Check if the token is expired
 */
export const isTokenExpired = (token: string): boolean => {
  const decoded = decodeToken(token);
  if (!decoded || !decoded.exp) {
    return true; // If there's no expiration, consider it expired
  }

  const currentTime = Math.floor(Date.now() / 1000);
  return decoded.exp < currentTime;
};

/**
 * Get the token expiration time
 */
export const getTokenExpiration = (token: string): Date | null => {
  const decoded = decodeToken(token);
  if (!decoded || !decoded.exp) {
    return null;
  }

  return new Date(decoded.exp * 1000);
};

// Server-side authentication helper
export async function getAuthSession() {
  // This is a placeholder implementation
  // In a real app, you'd use Next.js server components to access cookies
  // and verify the JWT token on the server side
  if (typeof window === 'undefined') {
    // Server-side code
    try {
      // For now, return a mock session
      // In a real implementation, you would:
      // 1. Get the token from cookies
      // 2. Verify the JWT
      // 3. Fetch user details from the database
      return {
        user: { email: 'server@example.com', name: 'Server User' },
        isValid: true,
      };
    } catch (error) {
      console.error('Error getting auth session:', error);
      return null;
    }
  } else {
    // Client-side code - use the existing client-side functions
    const token = getToken();
    if (!token || isTokenExpired(token)) {
      return null;
    }

    const decoded = decodeToken(token);
    return {
      user: { email: decoded?.email || '', name: decoded?.email?.split('@')[0] },
      isValid: true,
    };
  }
}