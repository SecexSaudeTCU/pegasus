##########################################################################################################################################################
#  SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM  SIM  #
##########################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do SIM para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e Microsoft SQL Server
com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do SIM definido nas classes abaixo
Base = declarative_base()

# Tabela das Declarações de Óbito (DO) (tabela principal do banco de dados)
class DOBR(Base):
    __tablename__ = 'dobr'
    __table_args__ = {'schema': 'sim'}
    # Aqui se define as colunas para a tabela "dnbr" e assim por diante com as outras "class"
    # Note que cada coluna é também um atributo de instância e assim por diante com as outras "class"
    NUMERODO = Column(String(8))                                                # Logical key
    UF_DO = Column(String(2))                                                   # Atributo
    ANO_DO = Column(Integer)                                                    # Atributo
    CODINST = Column(String(18))                                                # Atributo
    TIPOBITO_ID = Column(String(2), ForeignKey('sim.tipobito.ID'))              # Foreign key
    tipobito = relationship('TIPOBITO')
    DTOBITO = Column(Date)                                                      # Atributo
    HORAOBITO = Column(String(4))                                               # Atributo
    NUMSUS = Column(String(15))                                                 # Atributo
    NATURALE_ID = Column(String(3), ForeignKey('sim.naturale.ID'))              # Foreign key
    naturale = relationship('NATURALE')
    CODMUNNATU_ID = Column(String(6), ForeignKey('sim.codmunnatu.ID'))          # Foreign key
    codmunnatu = relationship('CODMUNNATU')
    DTNASC = Column(Date)                                                       # Atributo
    IDADE = Column(Float)                                                       # Atributo
    SEXO = Column(String(2))                                                    # Atributo
    RACACOR_ID = Column(String(2), ForeignKey('sim.racacor.ID'))                # Foreign key
    racacor = relationship('RACACOR')
    ESTCIV_ID = Column(String(2), ForeignKey('sim.estciv.ID'))                  # Foreign key
    estciv = relationship('ESTCIV')
    ESC_ID = Column(String(2), ForeignKey('sim.esc.ID'))                        # Foreign key
    esc = relationship('ESC')
    ESC2010_ID = Column(String(2), ForeignKey('sim.esc2010.ID'))                # Foreign key
    esc2010 = relationship('ESC2010')
    OCUP_ID = Column(String(6), ForeignKey('sim.ocup.ID'))                      # Foreign key
    ocup = relationship('OCUP')
    CODMUNRES_ID = Column(String(6), ForeignKey('sim.codmunres.ID'))            # Foreign key
    codmunres = relationship('CODMUNRES')
    LOCOCOR_ID = Column(String(2), ForeignKey('sim.lococor.ID'))                # Foreign key
    lococor = relationship('LOCOCOR')
    CODESTAB_ID = Column(String(7), ForeignKey('sim.codestab.ID'))              # Foreign key
    codestab = relationship('CODESTAB')
    CODMUNOCOR_ID = Column(String(6), ForeignKey('sim.codmunocor.ID'))          # Foreign key
    codmunocor = relationship('CODMUNOCOR')
    TPMORTEOCO_ID = Column(String(2), ForeignKey('sim.tpmorteoco.ID'))          # Foreign key
    tpmorteoco = relationship('TPMORTEOCO')
    ASSISTMED = Column(Numeric)                                                 # Atributo
    EXAME = Column(Numeric)                                                     # Atributo
    CIRURGIA = Column(Numeric)                                                  # Atributo
    NECROPSIA =Column(Numeric)                                                  # Atributo
    LINHAA = Column(String(66))                                                 # Atributo
    LINHAB = Column(String(66))                                                 # Atributo
    LINHAC = Column(String(66))                                                 # Atributo
    LINHAD = Column(String(66))                                                 # Atributo
    LINHAII = Column(String(66))                                                # Atributo
    CAUSABAS_ID = Column(String(4), ForeignKey('sim.causabas.ID'))              # Foreign key
    causabas = relationship('CAUSABAS')
    CRM = Column(String(15))                                                    # Atributo
    DTATESTADO = Column(Date)                                                   # Atributo
    CIRCOBITO_ID = Column(String(2), ForeignKey('sim.circobito.ID'))            # Foreign key
    circobito = relationship('CIRCOBITO')
    ACIDTRAB = Column(Numeric)                                                  # Atributo
    FONTE_ID = Column(String(2), ForeignKey('sim.fonte.ID'))                    # Foreign key
    fonte = relationship('FONTE')
    TPPOS = Column(Numeric)                                                     # Atributo
    DTINVESTIG = Column(Date)                                                   # Atributo
    CAUSABAS_O_ID = Column(String(4), ForeignKey('sim.causabas_o.ID'))          # Foreign key
    causabas_o = relationship('CAUSABAS_O')
    DTCADASTRO = Column(Date)                                                   # Atributo
    ATESTANTE_ID = Column(String(2), ForeignKey('sim.atestante.ID'))            # Foreign key
    atestante = relationship('ATESTANTE')
    FONTEINV_ID = Column(String(2), ForeignKey('sim.fonteinv.ID'))              # Foreign key
    fonteinv = relationship('FONTEINV')
    DTRECEBIM = Column(Date)                                                    # Atributo
    ATESTADO = Column(String(66))                                               # Atributo
    ESCMAEAGR1_ID = Column(String(2), ForeignKey('sim.escmaeagr1.ID'))          # Foreign key
    escmaeagr1 = relationship('ESCMAEAGR1')
    ESCFALAGR1_ID = Column(String(2), ForeignKey('sim.escfalagr1.ID'))          # Foreign key
    escfalagr1 = relationship('ESCFALAGR1')
    STDOEPIDEM = Column(Numeric)                                                # Atributo
    STDONOVA = Column(Numeric)                                                  # Atributo
    DIFDATA = Column(Float)                                                     # Atributo
    DTCADINV = Column(Date)                                                     # Atributo
    TPOBITOCOR_ID = Column(String(2), ForeignKey('sim.tpobitocor.ID'))          # Foreign key
    tpobitocor = relationship('TPOBITOCOR')
    DTCONINV = Column(Date)                                                     # Atributo
    FONTENTREV = Column(Numeric)                                                # Atributo
    FONTEAMBUL = Column(Numeric)                                                # Atributo
    FONTEPRONT = Column(Numeric)                                                # Atributo
    FONTESVO = Column(Numeric)                                                  # Atributo
    FONTEIML = Column(Numeric)                                                  # Atributo
    FONTEPROF = Column(Numeric)                                                 # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela do tipo de óbito
class TIPOBITO(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'tipobito'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela do local de nascimento
class NATURALE(Base):
    __tablename__ = 'naturale'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    LOCAL = Column(String(66))                                                  # Logical key

# Tabela dos municípios de naturalidade
class CODMUNNATU(Base):
    __tablename__ = 'codmunnatu'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sim.ufcod.ID'))                    # Foreign key
    ufcod = relationship('UFCOD')
    AMAZONIA = Column(String(66))                                               # Atributo
    FRONTEIRA = Column(String(66))                                              # Atributo
    CAPITAL = Column(String(66))                                                # Atributo
    LATITUDE = Column(Float)                                                    # Atributo
    LONGITUDE = Column(Float)                                                   # Atributo
    ALTITUDE = Column(Float)                                                    # Atributo
    AREA = Column(Float)                                                        # Atributo
    ANOINST = Column(String(66))                                                # Atributo
    ANOEXT = Column(String(66))                                                 # Atributo
    SUCESSOR = Column(String(66))                                               # Atributo

# Tabela das raças
class RACACOR(Base):
    __tablename__ = 'racacor'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela de estados civis do falecido
class ESTCIV(Base):
    __tablename__ = 'estciv'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    SITUACAO = Column(String(66))                                               # Logical key

# Tabela das faixas de anos de instrução
class ESC(Base):
    __tablename__ = 'esc'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    FAIXA_DE_ANOS_INSTRUCAO = Column(String(66))                                # Logical key

# Tabela das escolaridades
class ESC2010(Base):
    __tablename__ = 'esc2010'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESCOLARIDADE = Column(String(66))                                           # Logical key

# Tabela das ocupações
class OCUP(Base):
    __tablename__ = 'ocup'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Tabela dos municípios de residência
class CODMUNRES(Base):
    __tablename__ = 'codmunres'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sim.ufcod.ID'))                    # Foreign key
    ufcod = relationship('UFCOD')
    AMAZONIA = Column(String(66))                                               # Atributo
    FRONTEIRA = Column(String(66))                                              # Atributo
    CAPITAL = Column(String(66))                                                # Atributo
    LATITUDE = Column(Float)                                                    # Atributo
    LONGITUDE = Column(Float)                                                   # Atributo
    ALTITUDE = Column(Float)                                                    # Atributo
    AREA = Column(Float)                                                        # Atributo
    ANOINST = Column(String(66))                                                # Atributo
    ANOEXT = Column(String(66))                                                 # Atributo
    SUCESSOR = Column(String(66))                                               # Atributo

# Tabela dos lugares de falecimento
class LOCOCOR(Base):
    __tablename__ = 'lococor'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    LUGAR = Column(String(66))                                                  # Logical key

# Tabela dos estabelecimentos
class CODESTAB(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'codestab'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(7), primary_key=True)                                    # Primary key
    DESCESTAB = Column(String(66))                                              # Logical key
    ESFERA = Column(String(66))                                                 # Atributo
    REGIME = Column(String(66))                                                 # Atributo

# Tabela dos municípios de ocorrência
class CODMUNOCOR(Base):
    __tablename__ = 'codmunocor'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sim.ufcod.ID'))                    # Foreign key
    ufcod = relationship('UFCOD')
    AMAZONIA = Column(String(66))                                               # Atributo
    FRONTEIRA = Column(String(66))                                              # Atributo
    CAPITAL = Column(String(66))                                                # Atributo
    LATITUDE = Column(Float)                                                    # Atributo
    LONGITUDE = Column(Float)                                                   # Atributo
    ALTITUDE = Column(Float)                                                    # Atributo
    AREA = Column(Float)                                                        # Atributo
    ANOINST = Column(String(66))                                                # Atributo
    ANOEXT = Column(String(66))                                                 # Atributo
    SUCESSOR = Column(String(66))                                               # Atributo

# Tabela de momentos de óbito
class TPMORTEOCO(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'tpmorteoco'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    EPOCA_MORTE = Column(String(66))                                            # Logical key

# Tabela de causas básicas da DO
class CAUSABAS(Base):
    __tablename__ = 'causabas'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    DOENCA = Column(String(66))                                                 # Logical key

# Tabela das circunstâncias de morte violenta
class CIRCOBITO(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'circobito'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CIRCUNSTANCIA = Column(String(66))                                          # Logical key

# Tabela de fontes de informação
class FONTE(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'fonte'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ORIGEM = Column(String(66))                                                 # Logical key

# Tabela de causas básicas originais da morte
class CAUSABAS_O(Base):
    __tablename__ = 'causabas_o'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    DOENCA = Column(String(66))                                                 # Logical key

# Tabela se o atestador do falecimento é médico ou outro
class ATESTANTE(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'atestante'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ATESTADOR = Column(String(66))                                              # Logical key

# Tabela de fontes de investigação
class FONTEINV(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'fonteinv'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ORIGEM = Column(String(66))                                                 # Logical key

# Tabela de escolaridades da mãe
class ESCMAEAGR1(Base):
    __tablename__ = 'escmaeagr1'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESCOLARIDADE = Column(String(66))                                           # Logical key

# Tabela de escolaridades do falecido
class ESCFALAGR1(Base):
    __tablename__ = 'escfalagr1'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESCOLARIDADE = Column(String(66))                                           # Logical key

# Tabela de momentos de óbito
class TPOBITOCOR(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'tpobitocor'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    EPOCA_MORTE = Column(String(66))                                            # Logical key

# Tabela de Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'sim'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'sim'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
