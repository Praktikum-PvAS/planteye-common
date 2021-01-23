import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="planteye-common",
    version="1.0.1",
    author="Valentin Khaydarov",
    author_email="valentin.khaydarov@gmail.com",
    description="Common function for planyeye",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vkhaydarov/planyeye-common",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)