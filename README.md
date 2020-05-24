# PegaSUS
Plataforma Eletrônica de Governança e Accountability do SUS
<p style="text-align: justify">
Este projeto está dividido em n módulos. Um dos módulos já implementado,
denominado dados, permite criar um *schema* de um banco de dados PostgreSQL local,
já criado previamente, e insere nele dados das bases de dados (do Datasus) CNES,
SIH, SIA, SIM, SINASC ou SINAN (por enquanto). Esse módulo também permite que se
faça a leitura de arquivos principais de dados (em formato *.dbc*) e de arquivos
auxiliares de dados dessas bases do Datasus (em formato *.dbf* ou *.cnv*)
diretamente como objetos pandas DataFrame. Essa segunda opção é recomendada no
caso de consulta mais pontual.

O outro módulo deste projeto já implementado se denomina analise e realiza
análise de anomalias estatísticas das bases de dados do SIH e SIA...</p>

## Instalação do módulo dados
<p style="text-align: justify">
Para o uso do módulo dados por estrutura de banco de dados é necessário instalar  
o SGBD [PostgreSQL](https://www.postgresql.org/download/) e é recomendável ter
um software para gerenciamento de banco de dados, tal como [pgAdmin](https://www.pgadmin.org/download/) ou
[DBeaver Community](https://dbeaver.io/), para eventualmente visualizar os
dados no banco de dados.</p>

Para uso do módulo dados em plataformas Linux ou MacOS é necessária a instalação
da dependência libffi:
1) Linux: `sudo apt install libffi-dev`
2) MacOS: `brew install libffi`

Para uso em plataformas Windows, é necessária a instalação do programa executável
(*.exe*) dbf2dbc que faz parte do programa TabWin do Datasus. Faça o download da
pasta *.zip* Tab415 selecionando o link "Tab415.zip" contido [aqui](http://www2.datasus.gov.br/DATASUS/index.php?area=060805&item=3)
e presente na primeira linha da coluna "Nome" da tabela que aparece nessa página.
Descompacte a pasta "Tab415.zip", copie o executável "dbf2dbc.exe" e o arquivo
"IMPBORL.DLL" (a pasta descompactada "Tab415" e seu *.zip* podem então ser
deletados) para uma pasta nova denominada dbf2dbc no seu *home directory*, o
qual toma a seguinte forma em plataformas Windows:</p>

```C:\Users\username\```
<p style="text-align: justify">
Em seguida se pode instalar o módulo dados através do comando (em plataformas Linux):</p>

```$ sudo pip install pegasus-dados```

## Exemplos do uso do módulo dados
...

## Exemplos do uso do módulo analise
...</div>
