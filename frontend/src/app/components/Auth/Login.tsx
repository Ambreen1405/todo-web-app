"use client";

import React, { useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "../../context/auth";

interface LoginFormData {
  email: string;
  password: string;
}

const Login: React.FC = () => {
  const [formData, setFormData] = useState<LoginFormData>({
    email: "",
    password: "",
  });
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});

  const router = useRouter();
  const { login } = useAuth();

  const validateField = (name: string, value: string) => {
    switch (name) {
      case "email":
        if (!value) {
          return "Email is required";
        }
        if (!/\S+@\S+\.\S+/.test(value)) {
          return "Email is invalid";
        }
        return "";
      case "password":
        if (!value) {
          return "Password is required";
        }
        if (value.length < 6) {
          return "Password must be at least 6 characters";
        }
        return "";
      default:
        return "";
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: value,
    }));

    // Clear error for this field when user types
    if (errors[name]) {
      setErrors((prev) => {
        const newErrors = { ...prev };
        delete newErrors[name];
        return newErrors;
      });
    }
  };

  const validateForm = () => {
    const newErrors: Record<string, string> = {};

    Object.entries(formData).forEach(([key, value]) => {
      const fieldError = validateField(key, value);
      if (fieldError) {
        newErrors[key] = fieldError;
      }
    });

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!validateForm()) {
      return;
    }

    setLoading(true);

    try {
      const success = await login(formData.email, formData.password);

      if (success) {
        // Trigger event to update dashboard stats after login
        window.dispatchEvent(new CustomEvent("task-updated"));

        // Redirect to dashboard or wherever appropriate
        router.push("/dashboard");
      } else {
        setError("Login failed. Please check your credentials and try again.");
      }
    } catch (err) {
      setError("An error occurred during login.");
      console.error("Login error:", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      className="min-h-screen flex items-center justify-center bg-gradient-to-br from-teal-400 via-yellow-100 to-teal-600  p-4">
      <div className="w-full max-w-md">
        <div className="bg-white rounded-2xl shadow-xl p-8">
          {/* Tabs */}
          <div className="flex mb-6 border-b">
            <button className="flex-1 py-2 text-center font-semibold text-teal-400 border-b-2 border-teal-400 text-sm sm:text-base">
              Login
            </button>
            <button
              className="flex-1 py-2 text-center font-semibold text-gray-500 hover:text-teal-400 transition-colors text-sm sm:text-base"
              onClick={() => (window.location.href = "/register")}
            >
              Signup
            </button>
          </div>

          {/* Title */}
          <div className="text-center mb-8">
            <h1 className="text-2xl font-bold text-gray-800">Login Form</h1>
            <p className="text-gray-600 text-[0.8rem] mt-2">
              Welcome back! Please sign in to continue
            </p>
          </div>

          <form className="space-y-6" onSubmit={handleSubmit}>
            {error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-3">
                <p className="text-red-600 text-sm text-center">{error}</p>
              </div>
            )}

            <div>
              <label
                htmlFor="email"
                className="block text-sm font-medium text-gray-700 mb-2"
              >
                Email Address
              </label>
              <input
                id="email"
                name="email"
                type="email"
                autoComplete="email"
                required
                className={`w-full px-4 py-3 border rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all text-sm sm:text-base ${
                  errors.email ? "border-red-300" : "border-gray-300"
                }`}
                placeholder="Enter your email"
                value={formData.email}
                onChange={handleChange}
              />
              {errors.email && (
                <p className="mt-1 text-sm text-red-600">{errors.email}</p>
              )}
            </div>

            <div>
              <label
                htmlFor="password"
                className="block text-sm font-medium text-gray-700 mb-2"
              >
                Password
              </label>
              <input
                id="password"
                name="password"
                type="password"
                autoComplete="current-password"
                required
                className={`w-full px-4 py-3 border rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all text-sm sm:text-base ${
                  errors.password ? "border-red-300" : "border-gray-300"
                }`}
                placeholder="Enter your password"
                value={formData.password}
                onChange={handleChange}
              />
              {errors.password && (
                <p className="mt-1 text-sm text-red-600">{errors.password}</p>
              )}
            </div>

            <div className="text-right">
              <a
                href="#"
                className="text-sm text-teal-400 hover:text-teal-500 transition-colors"
              >
                Forgot password?
              </a>
            </div>

            <button
              type="submit"
              disabled={loading}
              className="w-full bg-gradient-to-br from-teal-400 via-yellow-200 to-teal-300  text-black py-3 rounded-lg font-semibold hover:from-teal-500 hover:via-yellow-300 hover:to-teal-400 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-2 transition-all duration-200 transform hover:scale-[1.02] text-sm sm:text-base"
            >
              {loading ? (
                <div className="flex items-center justify-center">
                  <svg
                    className="animate-spin -ml-1 mr-3 h-5 w-5 text-black"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                  >
                    <circle
                      className="opacity-25"
                      cx="12"
                      cy="12"
                      r="10"
                      stroke="currentColor"
                      strokeWidth="4"
                    ></circle>
                    <path
                      className="opacity-75"
                      fill="currentColor"
                      d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    ></path>
                  </svg>
                  Signing in...
                </div>
              ) : (
                "Login"
              )}
            </button>
          </form>

          <div className="mt-4 text-center">
            <p className="text-gray-600">
              Not a member?{" "}
              <a
                href="/register"
                className="font-semibold text-teal-300 hover:text-teal-500 transition-colors"
              >
                Signup now
              </a>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Login;
