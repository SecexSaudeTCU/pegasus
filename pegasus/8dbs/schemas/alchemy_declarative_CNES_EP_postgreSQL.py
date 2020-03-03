###########################################################################################################################################################################
# CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES_EP (Equipes) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do CNES_EP definido nas classes abaixo
Base = declarative_base()

# Tabela das Equipes (EP) (tabela principal do "sub" banco de dados CNES_EP)
class EPBR(Base):
    __tablename__ = 'epbr'
    __table_args__ = {'schema': 'cnes_ep'}
    # Aqui se define as colunas para a tabela eqbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes_ep.cnes.ID'))                  # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_EP = Column(String(2))                                                   # Atributo
    ANO_EP = Column(Integer)                                                    # Atributo
    MES_EP = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes_ep.codufmun.ID'))          # Foreign key
    codufmun = relationship('CODUFMUN')
    IDEQUIPE_ID = Column(String(18), ForeignKey('cnes_ep.idequipe.ID'))         # Foreign key
    idequipe = relationship('IDEQUIPE')
    TIPOEQP_ID = Column(String(2), ForeignKey('cnes_ep.tipoeqp.ID'))            # Foreign key
    tipoeqp = relationship('TIPOEQP')
    NOME_EQP = Column(String(60))                                               # Atributo
    IDAREA_ID = Column(String(10), ForeignKey('cnes_ep.idarea.ID'))             # Foreign key
    idarea = relationship('IDAREA')
    NOMEAREA = Column(String(60))                                               # Atributo
    IDSEGM_ID = Column(String(8), ForeignKey('cnes_ep.idsegm.ID'))             # Foreign key
    idsegm = relationship('IDSEGM')
    DESCSEGM = Column(String(60))                                               # Atributo
    TIPOSEGM_ID = Column(String(2), ForeignKey('cnes_ep.tiposegm.ID'))          # Foreign key
    tiposegm = relationship('TIPOSEGM')
    DT_ATIVA = Column(Date)                                                     # Atributo
    DT_DESAT = Column(Date)                                                     # Atributo
    QUILOMBO = Column(Numeric)                                                  # Atributo
    ASSENTAD = Column(Numeric)                                                  # Atributo
    POPGERAL = Column(Numeric)                                                  # Atributo
    ESCOLA = Column(Numeric)                                                    # Atributo
    INDIGENA = Column(Numeric)                                                  # Atributo
    PRONASCI = Column(Numeric)                                                  # Atributo
    MOTDESAT_ID = Column(String(2), ForeignKey('cnes_ep.motdesat.ID'))          # Foreign key
    motdesat = relationship('MOTDESAT')
    TPDESAT_ID = Column(String(2), ForeignKey('cnes_ep.tpdesat.ID'))            # Foreign key
    tpdesat = relationship('TPDESAT')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes_ep'}
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
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes_ep.ufcod.ID'))                # Foreign key
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

# Tabelas da identificação de equipes
class IDEQUIPE(Base):
    __tablename__ = 'idequipe'
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(18), primary_key=True)                                   # Primary key
    NOME_EQUIPE = Column(String(66))                                            # Logical key

# Tabela dos tipos de equipe
class TIPOEQP(Base):
    __tablename__ = 'tipoeqp'
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela da identificação de áreas
class IDAREA(Base):
    __tablename__ = 'idarea'
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(10), primary_key=True)                                   # Primary key
    NOME_AREA = Column(String(66))                                              # Logical key

# Tabela da identificação de segmentos
class IDSEGM(Base):
    __tablename__ = 'idsegm'
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(8), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(66))                                              # Logical key

# Tabela dos tipos de segmento
class TIPOSEGM(Base):
    __tablename__ = 'tiposegm'
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos motivos de desativação
class MOTDESAT(Base):
    __tablename__ = 'motdesat'
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    MOTIVO = Column(String(66))                                                 # Logical key

# Tabela dos tipos de desativação
class TPDESAT(Base):
    __tablename__ = 'tpdesat'
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes_ep'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes_ep'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
