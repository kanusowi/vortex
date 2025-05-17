module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/tests'],
  transform: {
    '^.+\\.tsx?$': ['ts-jest', {
      tsconfig: 'tsconfig.json'
    }]
  },
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  collectCoverage: true,
  coverageDirectory: "coverage",
  coverageReporters: ["json", "lcov", "text", "clover"],
  testMatch: ['**/tests/**/*.test.ts'], // Pattern for test files
};
