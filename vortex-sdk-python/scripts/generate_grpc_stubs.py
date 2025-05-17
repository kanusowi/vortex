"""
Generates Python gRPC stubs from .proto files for the Vortex SDK.
"""
import subprocess
import sys
from pathlib import Path
import shutil
import os
import re

def post_process_generated_files(target_dir: Path):
    """
    Converts absolute imports in generated gRPC files to relative imports.
    e.g., 'from vortex.api.v1 import xxx_pb2' becomes 'from . import xxx_pb2'
    """
    print(f"Post-processing files in {target_dir}...")
    for filepath in target_dir.glob("*.py"):
        print(f"  Processing {filepath.name}")
        content = filepath.read_text()
        # Regex for 'from vortex.api.v1 import module'
        content = re.sub(
            r"from vortex\.api\.v1 import (\w+_pb2(?:_grpc)?)",
            r"from . import \1",
            content
        )
        # Regex for 'import vortex.api.v1.module as alias'
        content = re.sub(
            r"import vortex\.api\.v1\.(\w+_pb2(?:_grpc)?)\s+as\s+(\w+)",
            r"from . import \1 as \2",
            content
        )
        filepath.write_text(content)

    for filepath in target_dir.glob("*.pyi"): # Also process .pyi files
        print(f"  Processing {filepath.name} (stub)")
        content = filepath.read_text()
        content = re.sub(
            r"from vortex\.api\.v1 import (\w+_pb2(?:_grpc)?)",
            r"from . import \1",
            content
        )
        content = re.sub(
            r"import vortex\.api\.v1\.(\w+_pb2(?:_grpc)?)\s+as\s+(\w+)",
            r"from . import \1 as \2",
            content
        )
        filepath.write_text(content)
    print("Post-processing complete.")

def main():
    """Main function to generate gRPC stubs."""
    # Assuming this script is in vortex-sdk-python/scripts/
    project_root = Path(__file__).parent.parent.resolve()
    proto_source_dir = project_root / "proto" # Contains vortex/api/v1/*.proto
    
    # Output directory for generated stubs
    # This will create vortex_sdk/_grpc/vortex/api/v1/...
    output_dir = project_root / "vortex_sdk" / "_grpc"

    if not proto_source_dir.exists():
        print(f"Error: Proto source directory not found: {proto_source_dir}")
        sys.exit(1)

    # Ensure the base output directory exists, and clean it if it does
    if output_dir.exists():
        print(f"Cleaning existing output directory: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Path to protoc plugin from grpc_tools
    try:
        import grpc_tools.protoc # type: ignore
    except ImportError:
        print("Error: grpcio-tools is not installed. Please install it (`poetry install --with dev`).")
        sys.exit(1)

    proto_files_to_compile = [
        str(p.relative_to(proto_source_dir)) for p in proto_source_dir.glob("vortex/api/v1/*.proto")
    ]

    if not proto_files_to_compile:
        print(f"No .proto files found in {proto_source_dir / 'vortex/api/v1'}")
        sys.exit(1)

    print(f"Found proto files: {proto_files_to_compile}")
    print(f"Proto include path: {proto_source_dir}")
    print(f"Output base directory: {output_dir}")

    protoc_command = [
        sys.executable,  # Use the current Python interpreter
        "-m", "grpc_tools.protoc",
        f"-I{proto_source_dir}",
        f"--python_out={output_dir}",
        f"--pyi_out={output_dir}",  # For .pyi stub files
        f"--grpc_python_out={output_dir}",
    ] + proto_files_to_compile

    print(f"Running command: {' '.join(protoc_command)}")
    
    try:
        process = subprocess.run(protoc_command, capture_output=True, text=True, check=True)
        print("gRPC stubs generated successfully.")
        if process.stdout:
            print("protoc stdout:\n", process.stdout)
        if process.stderr:
            print("protoc stderr:\n", process.stderr) # protoc often outputs to stderr even on success
            
        # Create __init__.py files to make the generated directories packages
        # The output structure will be output_dir/vortex/api/v1/*.py
        # So we need __init__.py in output_dir/vortex and output_dir/vortex/api
        (output_dir / "vortex").mkdir(parents=True, exist_ok=True) # Ensure vortex dir exists if not created by protoc
        (output_dir / "vortex" / "__init__.py").touch(exist_ok=True)
        (output_dir / "vortex" / "api").mkdir(parents=True, exist_ok=True) # Ensure api dir exists
        (output_dir / "vortex" / "api" / "__init__.py").touch(exist_ok=True)
        
        # Ensure the specific v1 directory exists before trying to touch __init__.py or post-process
        v1_dir = output_dir / "vortex" / "api" / "v1"
        if not v1_dir.exists():
            # This case might happen if no protos were actually compiled into this specific path
            # or if protoc output structure is different than expected.
            # However, protoc should create it if there are files.
            print(f"Warning: Expected directory {v1_dir} not found after protoc. Skipping __init__.py and post-processing for it.")
        else:
            (v1_dir / "__init__.py").touch(exist_ok=True)
            print(f"Created __init__.py files in {output_dir / 'vortex'}")
            # Post-process the generated files in the v1 directory
            post_process_generated_files(v1_dir)

    except subprocess.CalledProcessError as e:
        print(f"Error generating gRPC stubs: {e}")
        print("stdout:\n", e.stdout)
        print("stderr:\n", e.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print("Error: python or grpc_tools.protoc not found. Make sure Python and grpcio-tools are in your PATH.")
        sys.exit(1)

if __name__ == "__main__":
    main()
