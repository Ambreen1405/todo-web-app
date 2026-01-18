"use client";

import React, {
  createContext,
  useContext,
  useState,
  useEffect,
  ReactNode,
} from "react";
import { apiClient } from "../api/client";
import { User } from "../types/user";
import { authApi } from "../api/auth";

interface AuthContextType {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<boolean>;
  logout: () => void;
  register: (
    email: string,
    name: string,
    password: string
  ) => Promise<{ success: boolean; error?: string }>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
}

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [user, setUser] = useState<User | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  // Check for existing token on initial load
  useEffect(() => {
    const storedToken = localStorage.getItem("token");
    if (storedToken) {
      setToken(storedToken);
      // Fetch user data to verify token validity and populate user info
      fetchUserData(storedToken);
    }

    // Listen for token expiration event
    const handleTokenExpiration = () => {
      logout();
    };

    window.addEventListener("token-expired", handleTokenExpiration);

    // Cleanup event listener on unmount
    return () => {
      window.removeEventListener("token-expired", handleTokenExpiration);
    };
  }, []);

  const fetchUserData = async (token: string) => {
    try {
      const response = await authApi.getCurrentUser(token);

      if (response.success && response.user) {
        setUser(response.user);
        setIsAuthenticated(true);
      } else {
        // Token is invalid, clear it
        localStorage.removeItem("token");
        setToken(null);
        setIsAuthenticated(false);
      }
    } catch (error) {
      console.error("Error fetching user data:", error);
      // Token is invalid, clear it
      localStorage.removeItem("token");
      setToken(null);
      setIsAuthenticated(false);
    }
  };

  // Function to check if token is expired
  const isTokenExpired = (token: string): boolean => {
    try {
      const payload = JSON.parse(atob(token.split(".")[1]));
      const currentTime = Math.floor(Date.now() / 1000);
      return payload.exp < currentTime;
    } catch (error) {
      console.error("Error checking token expiration:", error);
      return true; // Assume expired if we can't decode
    }
  };

  const login = async (email: string, password: string): Promise<boolean> => {
    try {
      const response = await authApi.login({ email, password });

      if (response.success && response.token) {
        const { token } = response;

        setToken(token);
        localStorage.setItem("token", token);

        // Fetch user data to populate user info
        await fetchUserData(token);

        return true;
      } else {
        console.error("Login failed:", response.error);
        return false;
      }
    } catch (error) {
      console.error("Login error:", error);
      return false;
    }
  };

  // const register = async (email: string, name: string, password: string): Promise<boolean> => {
  //   try {
  //     const response = await apiClient.post('/auth/register', { email, name, password });
  //     if (response.success) {
  //       // Auto-login after successful registration
  //       // return await login(email, password);
  //       return true;
  //     } else {
  //       console.error('Registration failed:', response.error);
  //       return false;
  //     }
  //   } catch (error) {
  //     console.error('Registration error:', error);
  //     return false;
  //   }
  // };
  const register = async (
    email: string,
    name: string,
    password: string
  ): Promise<{ success: boolean; error?: string }> => {
    try {
      const response = await authApi.register({
        email,
        name,
        password,
      });

      return response;
    } catch (error) {
      console.error("Registration error:", error);
      return {
        success: false,
        error: "Network error. Please try again.",
      };
    }
  };

  const logout = () => {
    setToken(null);
    setUser(null);
    setIsAuthenticated(false);
    localStorage.removeItem("token");
  };

  return (
    <AuthContext.Provider
      value={{ user, token, isAuthenticated, login, logout, register }}
    >
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
};
