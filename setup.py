import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name="pegasus-dados",
    version="0.1.0",
    packages=setuptools.find_packages(),
    package_data={
        '': ['*.xlsx', '*.c', '*.h', '*.o', '*.so', '*.md']
    },
    author="SecexSaudeTCU",
    author_email="ericlb@tcu.gov.br",
    description="Plataforma Eletrônica de Governança e Accountability do SUS - Módulo dados",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SecexSaudeTCU/pegasus/tree/master/pegasus/dados",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    python_requires='>=3.6',
    #setup_requires=['cffi>=1.0.0'],
    #cffi_modules=["dados/transform/extract/_build_readdbc.py:ffibuilder"],
    install_requires=['numpy', 'pandas', 'xlrd', 'dbfread', 'pyarrow', 'fastparquet', 'psycopg2', 'SQLAlchemy']
)
