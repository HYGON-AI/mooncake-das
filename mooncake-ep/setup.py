import os
import re
import subprocess
import sys
import sysconfig

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
import torch


torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.ep" + version_suffix


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
                f"-DPYTHON_EXECUTABLE={sys.executable}",
                f"-DMOONCAKE_EP_MODULE_NAME={module_leaf}",
                f"-DMOONCAKE_EXTENSION_SUFFIX={sysconfig.get_config_var('EXT_SUFFIX')}",
                f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={output_dir}",
                f"-DCMAKE_HIP_ARCHITECTURES={os.getenv('PYTORCH_ROCM_ARCH_LIST', '')}",
                f"-DTORCH_CXX11_ABI={int(torch._C._GLIBCXX_USE_CXX11_ABI)}",
                "-DCMAKE_BUILD_TYPE=Release",
            ]
        )
        subprocess.check_call(
            ["cmake", "--build", build_dir, "--target", "mooncake_ep", "-j",
             os.getenv("MAX_JOBS", "8")]
        )


setup(
    name=module_name,
    ext_modules=[Extension(module_name, sources=[])],
    cmdclass={"build_ext": CMakeBuild},
)
