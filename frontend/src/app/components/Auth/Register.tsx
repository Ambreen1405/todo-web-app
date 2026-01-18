"use client";

import React, { useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "../../context/auth";

interface RegisterFormData {
  email: string;
  name: string;
  password: string;
}

const Register: React.FC = () => {
  const [formData, setFormData] = useState<RegisterFormData>({
    email: "",
    name: "",
    password: "",
  });
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});

  const router = useRouter();
  const { register } = useAuth();

  const validateField = (name: string, value: string) => {
    switch (name) {
      case "name":
        if (!value) {
          return "Name is required";
        }
        if (value.length < 2) {
          return "Name must be at least 2 characters";
        }
        if (value.length > 255) {
          return "Name must be less than 255 characters";
        }
        return "";
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
        if (value.length < 8) {
          return "Password must be at least 8 characters";
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
      // const success = await register(formData.email, formData.name, formData.password);

      // if (success) {
      //   // Redirect to dashboard or wherever appropriate
      //   router.push('/login');
      // } else {
      //   setError('Registration failed. Email may already exist.');
      // }
      const result = await register(
        formData.email,
        formData.name,
        formData.password
      );

      if (result.success) {
        // Trigger event to update dashboard stats after registration
        window.dispatchEvent(new CustomEvent('task-updated'));

        router.push("/login"); // ya auto-login
      } else {
        setError(result.error ?? "Registration failed");
      }
    } catch (err) {
      setError("An error occurred during registration.");
      console.error("Registration error:", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-teal-400 via-yellow-100 to-teal-600  p-4">
      <div className="w-full max-w-md">
        <div className="bg-white rounded-2xl shadow-xl p-8">
          {/* Tabs */}
          <div className="flex mb-6 border-b">
            <button
              className="flex-1 py-2 text-center font-semibold text-gray-500 hover:text-teal-400 transition-colors"
              onClick={() => (window.location.href = "/login")}
            >
              Login
            </button>
            <button className="flex-1 py-2 text-center font-semibold text-teal-400 border-b-2 border-teal-400">
              Signup
            </button>
          </div>

          {/* Title */}
          <div className="text-center mb-5">
            <h1 className="text-2xl font-bold text-gray-800">Create Account</h1>
            <p className="text-gray-600 text-[0.8rem] mt-1">
              Join us today to get started
            </p>
          </div>

          <form className="space-y-6" onSubmit={handleSubmit}>
            {error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-3">
                <p className="text-red-600 text-sm text-center">
                  {typeof error === "string" ? error : "Something went wrong"}
                </p>
              </div>
            )}

            <div>
              <label
                htmlFor="name"
                className="block text-sm font-medium text-gray-700 mb-2"
              >
                Full Name
              </label>
              <input
                id="name"
                name="name"
                type="text"
                required
                className={`w-full px-4 py-3 border rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all ${
                  errors.name ? "border-red-300" : "border-gray-300"
                }`}
                placeholder="Enter your full name"
                value={formData.name}
                onChange={handleChange}
              />
              {errors.name && (
                <p className="mt-1 text-sm text-red-600">{errors.name}</p>
              )}
            </div>

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
                className={`w-full px-4 py-3 border rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all ${
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
                className={`w-full px-4 py-3 border rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all ${
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

            <button
              type="submit"
              disabled={loading}
              className="w-full bg-gradient-to-br from-teal-400 via-yellow-200 to-teal-300  text-black py-3 rounded-lg font-semibold hover:from-teal-500 hover:via-yellow-300 hover:to-teal-400 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-2 transition-all duration-200 transform hover:scale-[1.02] text-sm sm:text-base"
            >
              {loading ? (
                <div className="flex items-center justify-center">
                  <svg
                    className="animate-spin -ml-1 mr-3 h-5 w-5 text-white"
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
                  Creating account...
                </div>
              ) : (
                "Signup"
              )}
            </button>
          </form>

          <div className="mt-4 text-center">
            <p className="text-gray-600">
              Already a member?{" "}
              <a
                href="/login"
                className="font-semibold text-teal-400 hover:text-teal-500 transition-colors"
              >
                Login now
              </a>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Register;
