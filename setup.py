import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fpoptest",
    version="0.0.1",
    author="Chengqian Zhang",
    author_email="2043899742@qq.com",
    description="operators about first principle caculations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Chengqian-Zhang/FP_OPs",
    packages=setuptools.find_packages(),
    install_requires=[
        "pydflow",
        "lbg",
        "dpdata",
        "numpy",
        "dargs",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    provides=["fpoptest"],
    scripts=[]
)
