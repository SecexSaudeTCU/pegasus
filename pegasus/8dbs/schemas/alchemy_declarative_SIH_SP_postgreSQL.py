###########################################################################################################################################################################
# SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do SIH_SP (AIH Serviços Profissionais) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do SIH_SP definido nas classes abaixo
Base = declarative_base()

## Tabela das AIH Serviços Profissionais (SP) (tabela principal do "sub" banco de dados SIH_SP)
class SPBR(Base):
    __tablename__ = 'spbr'
    __table_args__ = {'schema': 'sih_sp'}
    # Aqui se define as colunas para a tabela spbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    SP_NAIH = Column(String(13))                                                # Logical key
    SPPROCREA_ID = Column(String(10), ForeignKey('sih_sp.spprocrea.ID'))        # Foreign key
    spprocrea = relationship('SPPROCREA')
    UF_SP = Column(String(2))                                                   # Atributo
    ANO_SP = Column(Integer)                                                    # Atributo
    MES_SP = Column(String(2))                                                  # Atributo
    SPGESTOR_ID = Column(String(6), ForeignKey('sih_sp.spgestor.ID'))           # Foreign key
    spgestor = relationship('SPGESTOR')
    SPCNES_ID = Column(String(7), ForeignKey('sih_sp.spcnes.ID'))               # Foreign key
    spcnes = relationship('SPCNES')
    SP_DTINTER = Column(Date)                                                   # Atributo
    SP_DTSAIDA = Column(Date)                                                   # Atributo
    SP_CPFCGC = Column(String(14))                                              # Atributo
    SPATOPROF_ID = Column(String(10), ForeignKey('sih_sp.spatoprof.ID'))        # Foreign key
    spatoprof = relationship('SPATOPROF')
    SP_QTD_ATO = Column(Float)                                                  # Atributo
    SP_PTSP = Column(Float)                                                     # Atributo
    SP_VALATO = Column(Float)                                                   # Atributo
    SPMHOSP_ID = Column(String(6), ForeignKey('sih_sp.spmhosp.ID'))             # Foreign key
    spmhosp = relationship('SPMHOSP')
    SPMPAC_ID = Column(String(6), ForeignKey('sih_sp.spmpac.ID'))               # Foreign key
    spmpac = relationship('SPMPAC')
    SP_DES_HOS = Column(Numeric)                                                # Atributo
    SP_DES_PAC = Column(Numeric)                                                # Atributo
    SPCOMPLEX_ID = Column(String(2), ForeignKey('sih_sp.spcomplex.ID'))         # Foreign key
    spcomplex = relationship('SPCOMPLEX')
    SPFINANC_ID = Column(String(2), ForeignKey('sih_sp.spfinanc.ID'))           # Foreign key
    spfinanc = relationship('SPFINANC')
    SPCOFAEC_ID = Column(String(6), ForeignKey('sih_sp.spcofaec.ID'))           # Foreign key
    spcofaec = relationship('SPCOFAEC')
    SPPFCBO_ID = Column(String(6), ForeignKey('sih_sp.sppfcbo.ID'))             # Foreign key
    sppfcbo = relationship('SPPFCBO')
    SP_PF_DOC = Column(String(11))                                              # Atributo
    SP_PJ_DOC = Column(String(7))                                               # Atributo
    INTPVAL_ID = Column(String(2), ForeignKey('sih_sp.intpval.ID'))             # Foreign key
    intpval = relationship('INTPVAL')
    SERVCLA_ID = Column(String(6), ForeignKey('sih_sp.servcla.ID'))             # Foreign key
    servcla = relationship('SERVCLA')
    SPCIDPRI_ID = Column(String(4), ForeignKey('sih_sp.spcidpri.ID'))           # Foreign key
    spcidpri = relationship('SPCIDPRI')
    SPCIDSEC_ID = Column(String(4), ForeignKey('sih_sp.spcidsec.ID'))           # Foreign key
    spcidsec = relationship('SPCIDSEC')
    SP_QT_PROC = Column(Float)                                                  # Atributo
    SP_U_AIH = Column(Numeric)                                                  # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de procedimento realizado
class SPPROCREA(Base):
    __tablename__ = 'spprocrea'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(10), primary_key=True)                                   # Primary key
    PROCEDIMENTO = Column(String(100))                                          # Logical key

# Tabela dos municípios gestor
class SPGESTOR(Base):
    __tablename__ = 'spgestor'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sih_sp.ufcod.ID'))                 # Foreign key
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

# Tabela dos estabelecimentos de saúde
class SPCNES(Base):
    __tablename__ = 'spcnes'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(7), primary_key=True)                                    # Primary key
    DESCESTAB = Column(String(66))                                              # Logical key

# Tabela dos tipos de procedimento referente a ato profissional
class SPATOPROF(Base):
    __tablename__ = 'spatoprof'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(10), primary_key=True)                                   # Primary key
    PROCEDIMENTO = Column(String(100))                                          # Logical key

# Tabela dos municípios de localização do estabelecimento
class SPMHOSP(Base):
    __tablename__ = 'spmhosp'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sih_sp.ufcod.ID'))                 # Foreign key
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

# Tabela dos municípios de residência do paciente
class SPMPAC(Base):
    __tablename__ = 'spmpac'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sih_sp.ufcod.ID'))                 # Foreign key
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

# Tabela dos níveis de complexidade do ato profissional
class SPCOMPLEX(Base):
    __tablename__ = 'spcomplex'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    COMPLEXIDADE = Column(String(66))                                           # Logical key

# Tabela dos tipos de recursos
class SPFINANC(Base):
    __tablename__ = 'spfinanc'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    FONTE = Column(String(66))                                                  # Logical key

# Tabela dos tipos de recursos FAEC
class SPCOFAEC(Base):
    __tablename__ = 'spcofaec'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    SUBFONTE = Column(String(66))                                               # Logical key

# Tabela das ocupações dos profissionais
class SPPFCBO(Base):
    __tablename__ = 'sppfcbo'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Tabela dos tipos de recursos
class INTPVAL(Base):
    __tablename__ = 'intpval'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO_VALOR = Column(String(66))                                             # Logical key

# Tabela de classificações dos serviços
class SERVCLA(Base):
    __tablename__ = 'servcla'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    CLASSIFICACAO = Column(String(100))                                         # Logical key

# Tabela dos tipos de diagnóstico principal
class SPCIDPRI(Base):
    __tablename__ = 'spcidpri'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    DIAGNOSTICO = Column(String(66))                                            # Logical key

# Tabela dos tipos de diagnóstico secundário
class SPCIDSEC(Base):
    __tablename__ = 'spcidsec'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    DIAGNOSTICO = Column(String(66))                                            # Logical key

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'sih_sp'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'sih_sp'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
