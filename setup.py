from setuptools import setup, find_packages


setup(
    name='magdacli',
    package_dir = {"": "src"},
    python_requires=">=3.8",
    version = '0.9',
    url='https://github.com/uwer/magda-cli-py',
    author='UR',
    author_email='ur@gmail.com',
    install_requires=[ 
        "pyjwt",
        "pyrest @ git+https://github.com/uwer/pyrest.git#egg=pyrest",
    ],
    
    packages=find_packages(where='src'),
)