import setuptools

lg = """
O projeto PegaSUS está dividido em n módulos. Um dos módulos já implementado,
denominado dados, permite criar um *schema* de um banco de dados PostgreSQL local,
já criado previamente, e insere nele dados das bases de dados (do Datasus) CNES,
SIH, SIA, SIM, SINASC ou SINAN (por enquanto). Esse módulo também permite que se
faça a leitura de arquivos principais de dados (em formato *.dbc*) e de arquivos
auxiliares de dados dessas bases do Datasus (em formato *.dbf* ou *.cnv*)
diretamente como objetos pandas DataFrame. Essa segunda opção é recomendada no
caso de consulta mais pontual.

## Instalação do pegasus-dados

Para o uso do módulo dados por estrutura de banco de dados é necessário instalar
o SGBD PostgreSQL (https://www.postgresql.org/download/) e é recomendável ter
um software para gerenciamento de banco de dados, tal como pgAdmin
(https://www.pgadmin.org/download/) ou DBeaver Community (https://dbeaver.io/),
para eventualmente visualizar os dados no banco de dados.

Para uso do módulo dados em plataformas Linux ou MacOS é necessária a instalação
da dependência libffi:
1) Linux: `$ sudo apt install libffi-dev`
2) MacOS: `$ brew install libffi`

Para uso em plataformas Windows, é necessária a instalação do programa executável
(*.exe*) dbf2dbc que faz parte do programa TabWin do Datasus. Faça o download da
pasta *.zip* Tab415 selecionando o link "Tab415.zip" contido na página da internet
http://www2.datasus.gov.br/DATASUS/index.php?area=060805&item=3 e presente na
primeira linha da coluna "Nome" da tabela que aparece nessa página. Descompacte
a pasta "Tab415.zip", copie o executável "dbf2dbc.exe" e o arquivo "IMPBORL.DLL"
(a pasta descompactada "Tab415" e seu *.zip* podem então ser deletados) para uma
pasta nova denominada dbf2dbc no seu *home directory*, o qual toma normalmente
a seguinte forma em plataformas Windows:

```C:\\Users\\username\\```

Em seguida se pode instalar o pegasus-dados através do comando (em plataformas Linux):

```$ sudo pip install pegasus-dados```

## Exemplos do uso do pegasus-dados
...
"""

setuptools.setup(
    name="pegasus-dados",
    version="0.1.2",
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
    cffi_modules=["dados/transform/extract/_build_readdbc.py:ffibuilder"],
    install_requires=['numpy', 'pandas', 'xlrd', 'dbfread', 'cffi>=1.0.0',
                      'pyarrow', 'fastparquet', 'psycopg2', 'SQLAlchemy']
)
