import os
import re
import subprocess
import sys
import sysconfig

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
import torch

use_musa = os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {"1", "ON", "TRUE", "YES"}
use_hcu = os.getenv("MOONCAKE_EP_USE_HCU", "").upper() in {"1", "ON", "TRUE", "YES"}
use_maca = (
    os.getenv("MOONCAKE_EP_USE_MACA", "").upper() in {"1", "ON", "TRUE", "YES"}
    or (hasattr(torch.version, "maca") and torch.version.maca is not None)
)
if use_hcu and not getattr(torch.version, "hip", None):
    raise RuntimeError("HCU EP requires a DTK PyTorch build with HIP support")
if use_hcu and (use_musa or use_maca):
    raise RuntimeError("HCU, MUSA, and MACA EP extension backends are mutually exclusive")
if use_musa:
    try:
        import torchada  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "torchada is required to build the MUSA EP extension. "
            "Please install it first using 'pip install torchada'."
        ) from e


from torch.utils.cpp_extension import (  # noqa: E402
    BuildExtension,
    CUDAExtension,
    CUDA_HOME,
)


torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.ep" + version_suffix

abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
current_dir = os.path.abspath(os.path.dirname(__file__))
repo_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sysroot_dir = os.path.join(repo_dir, ".deps", "sysroot", "usr")


def existing_dirs(*paths):
    return [path for path in paths if os.path.isdir(path)]


sysroot_include_dirs = existing_dirs(
    os.path.join(sysroot_dir, "include"),
    os.path.join(sysroot_dir, "include", "jsoncpp"),
    os.path.join(sysroot_dir, "include", "libnl3"),
)
sysroot_library_dirs = existing_dirs(
    os.path.join(sysroot_dir, "lib", "x86_64-linux-gnu"),
    os.path.join(sysroot_dir, "lib"),
)

abi_define = f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}"
cxx_args = [abi_define, "-std=c++20", "-O3", "-g0"]

cuda_libraries = ["ibverbs", "mlx5"]
cuda_library_dirs = []
include_dirs = [
    os.path.join(current_dir, "include"),
    os.path.join(current_dir, "../mooncake-transfer-engine/include"),
]

if use_musa:
    cuda_libraries = []
    musa_defines = ["-DUSE_MUSA", "-DMOONCAKE_EP_USE_MUSA=1"]
    cxx_args += musa_defines
    # torchada maps the "nvcc" key to "mcc".
    device_args = [
        abi_define,
        *musa_defines,
        "-std=c++20",
        "--cuda-gpu-arch=mp_21",
        "--cuda-gpu-arch=mp_31",
        "-O3",
    ]
elif use_maca:
    cuda_libraries = []
    cuda_library_dirs = sysroot_library_dirs.copy()
    include_dirs += sysroot_include_dirs
    maca_defines = ["-DUSE_MACA", "-DMOONCAKE_EP_USE_MACA=1"]
    cxx_args += maca_defines
    device_args = [
        abi_define,
        *maca_defines,
        "-std=c++20",
        "-O3",
    ]
else:
    cxx_args.append("-DUSE_CUDA")
    device_args = [
        abi_define,
        "-std=c++20",
        "-DUSE_CUDA",
        "-Xcompiler",
        "-O3",
        "-Xcompiler",
        "-g0",
    ]
    # Link against the CUDA driver stub library if available.
    if CUDA_HOME is not None:
        cuda_stub_dir = os.path.join(CUDA_HOME, "lib64", "stubs")
        cuda_stub_lib = os.path.join(cuda_stub_dir, "libcuda.so")
        if os.path.exists(cuda_stub_lib):
            cuda_libraries.insert(0, "cuda")
            cuda_library_dirs.append(cuda_stub_dir)


class CMakeBuild(build_ext):
    def build_extension(self, ext):
        source_dir = os.path.abspath(os.path.dirname(__file__))
        build_dir = os.path.abspath(self.build_temp)
        output_dir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        module_leaf = ext.name.rsplit(".", 1)[-1]
        os.makedirs(build_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        subprocess.check_call(
            [
                "cmake",
                "-S",
                source_dir,
                "-B",
                build_dir,
                "-DMOONCAKE_GPU_BACKEND=hcu",
                f"-DPYTHON_EXECUTABLE={sys.executable}",
                f"-DMOONCAKE_EP_MODULE_NAME={module_leaf}",
                f"-DMOONCAKE_EXTENSION_SUFFIX={sysconfig.get_config_var('EXT_SUFFIX')}",
                f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={output_dir}",
                f"-DCMAKE_HIP_ARCHITECTURES={os.getenv('PYTORCH_ROCM_ARCH', 'gfx936')}",
                f"-DTORCH_CXX11_ABI={int(torch._C._GLIBCXX_USE_CXX11_ABI)}",
                "-DCMAKE_BUILD_TYPE=Release",
            ]
        )
        subprocess.check_call(
            [
                "cmake",
                "--build",
                build_dir,
                "--target",
                "mooncake_ep",
                "-j",
                os.getenv("MAX_JOBS", "8"),
            ]
        )


if use_hcu:
    ext_modules = [Extension(module_name, sources=[])]
    build_ext_cmd = CMakeBuild
else:
    ext_modules = [
        CUDAExtension(
            name=module_name,
            include_dirs=include_dirs,
            sources=[
                "src/ep_py.cpp",
                "src/mooncake_ep_buffer.cpp",
                "src/mooncake_ep_kernel.cu",
            ],
            extra_compile_args={"cxx": cxx_args, "nvcc": device_args},
            libraries=cuda_libraries,
            library_dirs=cuda_library_dirs,
            extra_link_args=[
                "-Wl,-rpath,$ORIGIN",
                "-L" + os.path.join(current_dir, "../mooncake-wheel/mooncake"),
                "-Wl,--push-state,--no-as-needed",
                "-l:engine.so",
                "-Wl,--pop-state",
            ],
        )
    ]
    build_ext_cmd = BuildExtension


setup(
    name=module_name,
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext_cmd},
)
