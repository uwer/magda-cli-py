from setuptools import setup, find_packages


setup(
    name='magdacli',
    package_dir = {"": "src"},
    version = '0.9',
    url='https://github.com/uwer/magda-cli-py',
    author='UR',
    author_email='ur@gmail.com',
    install_requires=[
        "pyrest @ git+https://https://github.com/uwer/pyrest.git#egg=pyrest",
    ],
    
    packages=find_packages(where='src'),
)