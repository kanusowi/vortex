// .eslintrc.js
module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  plugins: [
    '@typescript-eslint',
  ],
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
  ],
  env: {
    node: true,
    jest: true,
  },
  rules: {
    // Specific rule adjustments can be made here.
    // For example, to be less strict about 'any' initially:
    // '@typescript-eslint/no-explicit-any': 'off',
    // To allow unused variables if they start with an underscore:
    '@typescript-eslint/no-unused-vars': ['warn', { 'argsIgnorePattern': '^_' }],
  },
  overrides: [
    {
      files: ['*.d.ts', 'src/_grpc/**/*.d.ts'], // Target .d.ts files, especially in _grpc
      rules: {
        '@typescript-eslint/ban-types': 'off', // Disable ban-types for .d.ts files
        '@typescript-eslint/no-unused-vars': 'off', // Disable no-unused-vars for .d.ts files
      },
    },
    {
      files: ['tests/**/*.ts'], // Target test files
      rules: {
        // Allow 'any' in test files for mocking flexibility, but issue a warning.
        // Could be set to 'off' if too many legitimate uses.
        '@typescript-eslint/no-explicit-any': 'warn', 
      },
    },
  ],
};
