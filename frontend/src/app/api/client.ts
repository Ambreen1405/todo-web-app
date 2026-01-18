/**
 * API Client Utilities
 * Centralized API client for making HTTP requests to the backend
 */

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "https://ambreen-dev-todo-app.hf.space";

export interface ApiResponse<T> {
  data?: T;
  error?: string;
  success: boolean;
}

class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<ApiResponse<T>> {
    const url = `${this.baseUrl}${endpoint}`;

    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          ...options.headers,
        },
        body:
          options.body && typeof options.body !== "string"
            ? JSON.stringify(options.body)
            : options.body,
      });

      // Handle 204 No Content responses (common for DELETE operations)
      if (response.status === 204) {
        return {
          success: true,
        };
      }

      const data = await response.json();

      if (!response.ok) {
        return {
          success: false,
          error: data.detail || "Request failed",
        };
      }
      // For successful non-204 responses, return the data
      return {
        success: true,
        data,
      };
    } catch (error: any) {
      return {
        error: error.message || "Network error occurred",
        success: false,
      };
    }
  }

  private sanitizeInput(input: any): any {
    if (typeof input === "string") {
      // Basic XSS prevention - remove script tags and other dangerous content
      return input
        .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, "")
        .replace(/javascript:/gi, "")
        .replace(/vbscript:/gi, "")
        .trim();
    } else if (Array.isArray(input)) {
      return input.map((item) => this.sanitizeInput(item));
    } else if (typeof input === "object" && input !== null) {
      const sanitized: any = {};
      for (const key in input) {
        if (Object.prototype.hasOwnProperty.call(input, key)) {
          sanitized[key] = this.sanitizeInput(input[key]);
        }
      }
      return sanitized;
    }
    return input;
  }

  private sanitizeOutput(output: any): any {
    // For now, just return the output as-is
    // In a real application, you might want to sanitize certain fields
    // that could contain user-generated content
    return output;
  }

  async get<T>(
    endpoint: string,
    headers: HeadersInit = {}
  ): Promise<ApiResponse<T>> {
    return this.request<T>(endpoint, {
      method: "GET",
      headers,
    });
  }

  async post<T>(
    endpoint: string,
    body: any,
    headers: HeadersInit = {}
  ): Promise<ApiResponse<T>> {
    return this.request<T>(endpoint, {
      method: "POST",
      headers,
      body,
    });
  }

  async put<T>(
    endpoint: string,
    body: any,
    headers: HeadersInit = {}
  ): Promise<ApiResponse<T>> {
    return this.request<T>(endpoint, {
      method: "PUT",
      headers,
      body: JSON.stringify(body),
    });
  }

  async delete<T>(
    endpoint: string,
    headers: HeadersInit = {}
  ): Promise<ApiResponse<T>> {
    return this.request<T>(endpoint, {
      method: "DELETE",
      headers,
    });
  }
}

export const apiClient = new ApiClient();
