###########################################################################################################################################################
# CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF #
###########################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES_PF (Profissionais) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do CNES_PF definido nas classes abaixo
Base = declarative_base()

# Tabela dos Profissionais (PF) (tabela principal do "sub" banco de dados CNES_PF)
class PFBR(Base):
    __tablename__ = 'pfbr'
    __table_args__ = {'schema': 'cnes_pf'}
    # Aqui se define as colunas para a tabela pfbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes_pf.cnes.ID'))                  # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_PF = Column(String(2))                                                   # Atributo
    ANO_PF = Column(Integer)                                                    # Atributo
    MES_PF = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes_pf.codufmun.ID'))          # Foreign key
    codufmun = relationship('CODUFMUN')
    CBO_ID = Column(String(6), ForeignKey('cnes_pf.cbo.ID'))                    # Logical/Foreign key
    cbo = relationship('CBO')
    CBOUNICO_ID = Column(String(6), ForeignKey('cnes_pf.cbounico.ID'))          # Foreign key
    cbounico = relationship('CBOUNICO')
    NOMEPROF = Column(String(60))                                               # Logical key
    CNS_PROF = Column(String(15))                                               # Logical key
    CONSELHO_ID = Column(String(2), ForeignKey('cnes_pf.conselho.ID'))          # Foreign key
    conselho = relationship('CONSELHO')
    REGISTRO = Column(String(13))                                               # Atributo
    VINCULAC_ID = Column(String(6), ForeignKey('cnes_pf.vinculac.ID'))          # Logical/Foreign key
    vinculac = relationship('VINCULAC')
    VINCUL_C = Column(Numeric)                                                  # Atributo
    VINCUL_A = Column(Numeric)                                                  # Atributo
    VINCUL_N = Column(Numeric)                                                  # Atributo
    PROF_SUS = Column(Numeric)                                                  # Atributo
    PROFNSUS = Column(Numeric)                                                  # Atributo
    HORAOUTR = Column(Float)                                                    # Atributo
    HORAHOSP = Column(Float)                                                    # Atributo
    HORA_AMB = Column(Float)                                                    # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes_pf'}
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
    __table_args__ = {'schema': 'cnes_pf'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes_pf.ufcod.ID'))                # Foreign key
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

# Tabela das especialidades dos profissionais
class CBO(Base):
    __tablename__ = 'cbo'
    __table_args__ = {'schema': 'cnes_pf'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Em verdade é a mesma coisa da tabela anterior
class CBOUNICO(Base):
    __tablename__ = 'cbounico'
    __table_args__ = {'schema': 'cnes_pf'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Tabeça dos conselhos de profissão
class CONSELHO(Base):
    __tablename__ = 'conselho'
    __table_args__ = {'schema': 'cnes_pf'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    DENOMINACAO = Column(String(66))                                            # Logical key

# Tabeça dos tipos de vínculos do profissional
class VINCULAC(Base):
    __tablename__ = 'vinculac'
    __table_args__ = {'schema': 'cnes_pf'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(100))                                             # Logical key

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes_pf'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes_pf'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
