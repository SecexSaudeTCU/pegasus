########################################################################################################################################################################
#  SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN   #
########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do SINAN para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""


# Incorpora o schema do banco de dados do SINAN definido nas classes abaixo
Base = declarative_base()

########################################################################################################################################################################
# SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG #
########################################################################################################################################################################

# Tabela de notificações dos agravos dengue e chikungunya (DENG)
class DENGBR(Base):
    __tablename__ = 'dengbr'
    __table_args__ = {'schema': 'sinan_deng'}
    # Aqui se define as colunas para a tabela dengbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    NU_NOTIFIC = Column(String(7))                                              # Logical key
    UF_DENG = Column(String(2))                                                 # Atributo
    ANO_DENG = Column(Integer)                                                  # Atributo
    TP_NOT = Column(String(2))                                                  # Atributo
    ID_AGRAVO = Column(String(3))                                               # Atributo
    MUNICIP_ID = Column(String(6), ForeignKey('sinan_deng.municip.ID'))         # Foreign key
    municip = relationship('MUNICIP')
    RES_CHIKS1 = Column(String(2))                                              # Atributo
    RES_CHIKS2 = Column(String(2))                                              # Atributo
    RESUL_PRNT = Column(String(2))                                              # Atributo
    RESUL_SORO = Column(String(2))                                              # Atributo
    RESUL_NS1 = Column(String(2))                                               # Atributo
    SOROTIPO = Column(String(2))                                                # Atributo
    HOSPITALIZ = Column(String(2))                                              # Atributo
    CLASSIFIN_ID = Column(String(2), ForeignKey('sinan_deng.classifin.ID'))     # Foreign key
    classifin = relationship('CLASSIFIN')
    EVOLUCAO = Column(String(2))                                                # Atributo
    GRAV_PULSO = Column(Numeric)                                                # Atributo
    GRAV_CONV = Column(Numeric)                                                 # Atributo
    GRAV_ENCH = Column(Numeric)                                                 # Atributo
    GRAV_INSUF = Column(Numeric)                                                # Atributo
    GRAV_TAQUI = Column(Numeric)                                                # Atributo
    GRAV_EXTRE = Column(Numeric)                                                # Atributo
    GRAV_HIPOT = Column(Numeric)                                                # Atributo
    GRAV_HEMAT = Column(Numeric)                                                # Atributo
    GRAV_MELEN = Column(Numeric)                                                # Atributo
    GRAV_METRO = Column(Numeric)                                                # Atributo
    GRAV_SANG = Column(Numeric)                                                 # Atributo
    GRAV_AST = Column(Numeric)                                                  # Atributo
    GRAV_MIOC = Column(Numeric)                                                 # Atributo
    GRAV_CONSC = Column(Numeric)                                                # Atributo
    GRAV_ORGAO = Column(Numeric)                                                # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos municípios de notificação
class MUNICIP(Base):
    __tablename__ = 'municip'
    __table_args__ = {'schema': 'sinan_deng'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sinan_deng.ufcod.ID'))             # Foreign key
    ufcod = relationship('UFCOD_DENG')
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

# Tabela dos tipos de classificação da dengue/chikungunya
class CLASSIFIN(Base):
    __tablename__ = 'classifin'
    __table_args__ = {'schema': 'sinan_deng'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CLASSIFICACAO = Column(String(66))                                          # Logical key

# Tabela dos Estados da RFB
class UFCOD_DENG(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'sinan_deng'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS_DENG(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'sinan_deng'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
