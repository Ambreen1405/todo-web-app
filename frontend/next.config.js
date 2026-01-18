/** @type {import('next').NextConfig} */
const nextConfig = {
  env: {
    NEXT_PUBLIC_API_BASE_URL: process.env.NEXT_PUBLIC_API_BASE_URL || 'https://ambreen-dev-todo-app.hf.space',
  },
  images: {
    domains: ['localhost', '0.0.0.0', 'ambreen-dev-todo-app.hf.space'],
  },
  transpilePackages: [],
};

module.exports = nextConfig;