###########################################################################################################################################################################
# CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES_GM (Gestão e Metas) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do CNES_GM definido nas classes abaixo
Base = declarative_base()

# Tabela de Gestão e Metas (GM) (tabela principal do "sub" banco de dados CNES_GM)
class GMBR(Base):
    __tablename__ = 'gmbr'
    __table_args__ = {'schema': 'cnes_gm'}
    # Aqui se define as colunas para a tabela gmbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes_gm.cnes.ID'))                  # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_GM = Column(String(2))                                                   # Atributo
    ANO_GM = Column(Integer)                                                    # Atributo
    MES_GM = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes_gm.codufmun.ID'))          # Foreign key
    codufmun = relationship('CODUFMUN')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    SGRUPHAB_ID = Column(String(4), ForeignKey('cnes_gm.sgruphab.ID'))          # Foreign key
    sgruphab = relationship('SGRUPHAB')
    CMPT_INI = Column(Date)                                                     # Atributo
    CMPT_FIM = Column(Date)                                                     # Atributo
    DTPORTAR = Column(Date)                                                     # Atributo
    PORTARIA = Column(String(50))                                               # Atributo
    MAPORTAR = Column(Date)                                                     # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes_gm'}
    ID = Column(String(7), primary_key=True)                                    # Primary key
    DESCESTAB = Column(String(66))                                              # Logical key
    RSOC_MAN = Column(String(66))                                               # Atributo
    CPF_CNPJ = Column(String(14))                                               # Atributo
    EXCLUIDO = Column(Numeric)                                                  # Atributo
    DATAINCL = Column(Date)                                                     # Atributo
    DATAEXCL = Column(Date)                                                     # Atributo

# Tabela dos municípios de localização de estabelecimentos
class CODUFMUN(Base):
    __tablename__ = 'codufmun'
    __table_args__ = {'schema': 'cnes_gm'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes_gm.ufcod.ID'))                # Foreign key
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

# Tabela dos tipos de gestão
class SGRUPHAB(Base):
    __tablename__ = 'sgruphab'
    __table_args__ = {'schema': 'cnes_gm'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    GESTAO = Column(String(100))                                                # Logical key

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes_gm'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes_gm'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
