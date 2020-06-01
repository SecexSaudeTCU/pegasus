import os
import setuptools

lg = """
The PegaSUS project is divided into n modules. One of the implemented modules,
called dados, allows to create a schema of a PostgreSQL database, previously
created in the user computer, and to insert data that belongs to the following
information health systems of the Brazilian Public Health System (SUS): CNES,
SIH, SIA, SIM, SINASC or SINAN (more to come). The dados module also also allows
to directly read main data files (in DBC format) and auxiliary data files (in
DBF or CNV format) as pandas DataFrame objects. This second option is recommended
in the case of more punctual consultation.

To use the dados module by means of a PostgreSQL database it is necessary to
install the PostgreSQL DBMS (https://www.postgresql.org/download/) and it is
recommended to have a database management software such as pgAdmin (https://www.pgadmin.org/download/)
or DBeaver Community (https://dbeaver.io/), to proper visualize the structured
data.

To use the dados module on Linux or MacOS platforms it is necessary to install
the libffi dependency:
1) Linux: `$ sudo apt install libffi-dev`
2) MacOS: `$ brew install libffi`

For use on Windows platforms it is necessary to install the executable program
(*.exe*) dbf2dbc, which is part of the Datasus Tabwin program. To accomplish this,
download the "Tab415.zip" folder by selecting the eponymous link contained in the
first line of the "Name" column of the table that appears in http://www2.datasus.gov.br/DATASUS/index.php?area=060805&item=3.
Unpack the "Tab415.zip" folder, copy the executable "dbf2dbc.exe" and the file
"IMPBORL.DLL" (the unpacked folder "Tab415" and its *.zip* can be deleted) to a
newly created folder named "dbf2dbc" in the root directory of the user's computer,
which generally has the following form on Windows platforms:

```C:\\```

Finally, the data module is installed using the command(on Linux platforms):

```$ sudo pip install pegasus-dados```
"""

if os.name == 'posix':

    setuptools.setup(
        name="pegasus-dados",
        version="0.2.0",
        packages=setuptools.find_packages(),
        package_data={
            '': ['*.xlsx', '*.c', '*.h', '*.o', '*.so', '*.md']
        },
        author="SecexSaudeTCU",
        author_email="ericlb@tcu.gov.br",
        description="Plataforma Eletrônica de Governança e Accountability do SUS - Módulo dados",
        long_description=lg,
        long_description_content_type="text/markdown",
        url="https://github.com/SecexSaudeTCU/pegasus/tree/master/pegasus/dados",
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License"
        ],
        python_requires='>=3.6',
        setup_requires=['cffi>=1.0.0'],
        cffi_modules=["pegasus/dados/transform/extract/_build_readdbc.py:ffibuilder"],
        install_requires=['numpy', 'pandas', 'xlrd', 'dbfread', 'cffi>=1.0.0',
                          'pyarrow', 'fastparquet', 'psycopg2', 'SQLAlchemy']
    )

elif os.name == 'nt':

    setuptools.setup(
        name="pegasus-dados",
        version="0.2.0",
        packages=setuptools.find_packages(),
        package_data={
            '': ['*.xlsx', '*.c', '*.h', '*.o', '*.so', '*.md']
        },
        author="SecexSaudeTCU",
        author_email="ericlb@tcu.gov.br",
        description="Plataforma Eletrônica de Governança e Accountability do SUS - Módulo dados",
        long_description=lg,
        long_description_content_type="text/markdown",
        url="https://github.com/SecexSaudeTCU/pegasus/tree/master/pegasus/dados",
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License"
        ],
        python_requires='>=3.6',
        install_requires=['numpy', 'pandas', 'xlrd', 'dbfread', 'pyarrow', 'fastparquet', 'psycopg2', 'SQLAlchemy']
    )
