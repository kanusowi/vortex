const { execSync } = require('child_process');
const path = require('path');
const fs = require('fs-extra');

const projectRoot = path.resolve(__dirname, '..');
// Assuming proto files are in a shared location, e.g., sibling to vortex-sdk-python
// Adjust this path if the proto files are located elsewhere relative to this SDK
const protoSourceDir = path.resolve(projectRoot, '../vortex-sdk-python/proto'); 
const protoIncludeDir = protoSourceDir; // The -I path for protoc

// Output directory for generated stubs within the TypeScript SDK
const outputDir = path.resolve(projectRoot, 'src', '_grpc');

if (!fs.existsSync(protoSourceDir)) {
  console.error(`Error: Proto source directory not found: ${protoSourceDir}`);
  process.exit(1);
}

// Clean and recreate the output directory
if (fs.existsSync(outputDir)) {
  console.log(`Cleaning existing output directory: ${outputDir}`);
  fs.rmSync(outputDir, { recursive: true, force: true });
}
fs.mkdirSync(outputDir, { recursive: true });

// Find all .proto files in the vortex/api/v1 subdirectory
// This assumes a structure like protoSourceDir/vortex/api/v1/*.proto
const protoFiles = [];
const specificProtoPath = path.join(protoSourceDir, 'vortex', 'api', 'v1');
if (fs.existsSync(specificProtoPath)) {
    fs.readdirSync(specificProtoPath).forEach(file => {
        if (file.endsWith('.proto')) {
            // protoc expects paths relative to the -I include path
            protoFiles.push(path.join('vortex', 'api', 'v1', file));
        }
    });
}


if (protoFiles.length === 0) {
  console.error(`No .proto files found in ${specificProtoPath}`);
  process.exit(1);
}

console.log(`Found proto files: ${protoFiles.join(', ')}`);
console.log(`Proto include path: ${protoIncludeDir}`);
console.log(`Output directory: ${outputDir}`);

// Path to plugins
// grpc_tools_node_protoc_plugin is for --grpc_out
// protoc-gen-ts (from ts-protoc-gen) is for --ts_out (messages)
// We need a way to generate service .d.ts files compatible with @grpc/grpc-js clients.
// grpc-tools itself should provide this.

const protocGenGrpcJsPath = path.resolve(projectRoot, 'node_modules', '.bin', 'grpc_tools_node_protoc_plugin');
// For TypeScript service definitions, grpc-tools can generate them directly
// when using the `grpc-js` library option.
// The `ts-protoc-gen` plugin is mainly for message definitions.

const checkAndGetExecutablePath = (basePath) => {
    if (fs.existsSync(basePath)) return basePath;
    const cmdPath = `${basePath}.cmd`; // For Windows
    if (fs.existsSync(cmdPath)) return cmdPath;
    return null;
};

const grpcJsPlugin = checkAndGetExecutablePath(protocGenGrpcJsPath);

if (!grpcJsPlugin) {
    console.error(`Error: grpc_tools_node_protoc_plugin not found.`);
    process.exit(1);
}

// Path to protoc-gen-ts from ts-protoc-gen package
const protocGenTsPath = path.resolve(projectRoot, 'node_modules', '.bin', 'protoc-gen-ts');
const tsPlugin = checkAndGetExecutablePath(protocGenTsPath);

// grpcJsPlugin is already checked earlier
if (!tsPlugin) {
    console.error(`Error: protoc-gen-ts not found (from ts-protoc-gen). Please ensure ts-protoc-gen is installed.`);
    process.exit(1);
}

const protocPath = path.resolve(projectRoot, 'node_modules', '.bin', 'grpc_tools_node_protoc');
const protocExecutable = checkAndGetExecutablePath(protocPath);

if (!protocExecutable) {
    console.error(`Error: grpc_tools_node_protoc not found at ${protocPath}. Please ensure grpc-tools is installed correctly.`);
    process.exit(1);
}

const protocCommand = [
  protocExecutable,
  // grpcJsPlugin is for the --grpc_out=grpc_js flag
  `--plugin=protoc-gen-grpc=${grpcJsPlugin}`, 
  // tsPlugin is for the --ts_out flag
  `--plugin=protoc-gen-ts=${tsPlugin}`,

  // Output JavaScript messages
  `--js_out=import_style=commonjs,binary:${outputDir}`,
  // Output JavaScript gRPC service clients for @grpc/grpc-js
  `--grpc_out=grpc_js:${outputDir}`, 
  
  // Output TypeScript definition files using ts-protoc-gen
  // Use service=grpc-web as this was the last known "successful" configuration
  // for generating some form of .d.ts files.
  `--ts_out=service=grpc-web:${outputDir}`,

  `-I${protoIncludeDir}`,
  ...protoFiles
].join(' ');

console.log(`Running command: ${protocCommand}`);

try {
  execSync(protocCommand, { stdio: 'inherit' });
  console.log('gRPC stubs generated successfully.');

  // Create __init__.js or index.ts files if needed to make directories modules
  // For TypeScript, usually the build process handles module resolution.
  // The output structure will be outputDir/vortex/api/v1/*.(js|d.ts)
  // Ensure these directories are treated as modules by TypeScript if necessary.
  // Often, no explicit __init__.js is needed for TS if using moduleResolution: "node".

} catch (error) {
  console.error(`Error generating gRPC stubs: ${error.message}`);
  if (error.stderr) console.error(`stderr: ${error.stderr.toString()}`);
  if (error.stdout) console.error(`stdout: ${error.stdout.toString()}`);
  process.exit(1);
}
