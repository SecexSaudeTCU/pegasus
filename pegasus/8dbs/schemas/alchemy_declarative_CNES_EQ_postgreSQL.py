###########################################################################################################################################################################
# CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES_EQ (Equipamentos) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do CNES_EQ definido nas classes abaixo
Base = declarative_base()

# Tabela dos Equipamentos (EQ) (tabela principal do "sub" banco de dados CNES_EQ)
class EQBR(Base):
    __tablename__ = 'eqbr'
    __table_args__ = {'schema': 'cnes_eq'}
    # Aqui se define as colunas para a tabela eqbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes_EQ.cnes.ID'))                  # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_EQ = Column(String(2))                                                   # Atributo
    ANO_EQ = Column(Integer)                                                    # Atributo
    MES_EQ = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes_eq.codufmun.ID'))          # Foreign key
    codufmun = relationship('CODUFMUN_EQ')
    TIPEQUIP_ID = Column(String(2), ForeignKey('cnes_eq.tipequip.ID'))          # Logical/Foreign key
    tipequip = relationship('TIPEQUIP')
    CODEQUIP_ID = Column(String(2), ForeignKey('cnes_eq.codequip.ID'))          # Logical/Foreign key
    codequip = relationship('CODEQUIP')
    QT_EXIST = Column(Float)                                                    # Logical key
    QT_USO = Column(Float)                                                      # Logical key
    IND_SUS = Column(Numeric)                                                   # Atributo
    ND_NSUS = Column(Numeric)                                                   # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes_eq'}
    ID = Column(String(7), primary_key=True)                                    # Primary key
    DESCESTAB = Column(String(66))                                              # Logical key
    RSOC_MAN = Column(String(66))                                               # Atributo
    CPF_CNPJ = Column(String(14))                                               # Atributo
    EXCLUIDO = Column(Numeric)                                                  # Atributo
    DATAINCL = Column(Date)                                                     # Atributo
    DATAEXCL = Column(Date)                                                     # Atributo

# Tabela dos municípios de localização de estabelecimentos
class CODUFMUN_EQ(Base):
    __tablename__ = 'codufmun'
    __table_args__ = {'schema': 'cnes_eq'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes_eq.ufcod.ID'))                # Foreign key
    ufcod = relationship('UFCOD_EQ')
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

# Tabela dos tipos de equipamentos
class TIPEQUIP(Base):
    __tablename__ = 'tipequip'
    __table_args__ = {'schema': 'cnes_eq'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos nomes dos equipamentos
class CODEQUIP(Base):
    __tablename__ = 'codequip'
    __table_args__ = {'schema': 'cnes_eq'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    DENOMINACAO = Column(String(66))                                            # Logical key

# Tabela dos Estados da RFB
class UFCOD_EQ(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes_eq'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS_EQ(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes_eq'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
