###########################################################################################################################################################################
# CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES_EF (Estabelecimentos Filantrópicos) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do CNES_EF definido nas classes abaixo
Base = declarative_base()

# Tabela dos Estabelecimentos Filantrópicos (EF) (tabela principal do "sub" banco de dados CNES_EF)
class EFBR(Base):
    __tablename__ = 'efbr'
    __table_args__ = {'schema': 'cnes_ef'}
    # Aqui se define as colunas para a tabela efbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes_ef.cnes.ID'))                  # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_EF = Column(String(2))                                                   # Atributo
    ANO_EF = Column(Integer)                                                    # Atributo
    MES_EF = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes_ef.codufmun.ID'))          # Foreign key
    codufmun = relationship('CODUFMUN')
    TPGESTAO_ID = Column(String(2), ForeignKey('cnes_ef.tpgestao.ID'))          # Foreign key
    tpgestao = relationship('TPGESTAO')
    PFPJ_ID = Column(String(2), ForeignKey('cnes_ef.pfpj.ID'))                  # Foreign key
    pfpj = relationship('PFPJ')
    CPF_CNPJ = Column(String(14))                                               # Atributo
    NIVDEP_ID = Column(String(2), ForeignKey('cnes_ef.nivdep.ID'))              # Foreign key
    nivdep = relationship('NIVDEP')
    CNPJ_MAN = Column(String(14))                                               # Atributo
    ESFERAA_ID = Column(String(2), ForeignKey('cnes_ef.esferaa.ID'))            # Foreign key
    esferaa = relationship('ESFERAA')
    RETENCAO_ID = Column(String(2), ForeignKey('cnes_ef.retencao.ID'))          # Foreign key
    retencao = relationship('RETENCAO')
    ATIVIDAD_ID = Column(String(2), ForeignKey('cnes_ef.atividad.ID'))          # Foreign key
    atividad = relationship('ATIVIDAD')
    NATUREZA_ID = Column(String(2), ForeignKey('cnes_ef.natureza.ID'))          # Foreign key
    natureza = relationship('NATUREZA')
    CLIENTEL_ID = Column(String(2), ForeignKey('cnes_ef.clientel.ID'))          # Foreign key
    clientel = relationship('CLIENTEL')
    TPUNID_ID = Column(String(2), ForeignKey('cnes_ef.tpunid.ID'))              # Foreign key
    tpunid = relationship('TPUNID')
    TURNOAT_ID = Column(String(2), ForeignKey('cnes_ef.turnoat.ID'))            # Foreign key
    turnoat = relationship('TURNOAT')
    NIVHIER_ID = Column(String(2), ForeignKey('cnes_ef.nivhier.ID'))            # Foreign key
    nivhier = relationship('NIVHIER')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    COD_CEP = Column(String(8))                                                 # Atributo
    VINC_SUS = Column(Numeric)                                                  # Atributo
    TPPREST_ID = Column(String(2), ForeignKey('cnes_ef.tpprest.ID'))            # Foreign key
    tpprest = relationship('TPPREST')
    SGRUPHAB_ID = Column(String(4), ForeignKey('cnes_ef.sgruphab.ID'))          # Foreign key
    sgruphab = relationship('SGRUPHAB')
    CMPT_INI = Column(Date)                                                     # Atributo
    CMPT_FIM = Column(Date)                                                     # Atributo
    DTPORTAR = Column(Date)                                                     # Atributo
    PORTARIA = Column(String(50))                                               # Atributo
    MAPORTAR = Column(Date)                                                     # Atributo
    NATJUR_ID = Column(String(4), ForeignKey('cnes_ef.natjur.ID'))              # Foreign key
    natjur = relationship('NATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes_ef'}
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
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes_ef.ufcod.ID'))                # Foreign key
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
class TPGESTAO(Base):
    __tablename__ = 'tpgestao'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    GESTAO = Column(String(66))                                                 # Logical key

# Tabela se o estabelecimento é pessoa física ou pessoa jurídica
class PFPJ(Base):
    __tablename__ = 'pfpj'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    PESSOA = Column(String(66))                                                 # Logical key

# Tabela do grau de independência do estabelecimento
class NIVDEP(Base):
    __tablename__ = 'nivdep'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos tipos de administração
class ESFERAA(Base):
    __tablename__ = 'esferaa'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ADMINISTRACAO = Column(String(66))                                          # Logical key

# Tabela dos tipos de retenção de tributos do estabelecimento
class RETENCAO(Base):
    __tablename__ = 'retencao'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    RETENCAO = Column(String(66))                                               # Logical key

# Tabela dos tipos de atividade de ensino, se houver
class ATIVIDAD(Base):
    __tablename__ = 'atividad'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ATIVIDADE = Column(String(66))                                              # Logical key

# Tabela da natureza do estabelecimento
class NATUREZA(Base):
    __tablename__ = 'natureza'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(66))                                               # Logical key

# Tabela dos tipos de fluxo de clientela
class CLIENTEL(Base):
    __tablename__ = 'clientel'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CLIENTELA = Column(String(66))                                              # Logical key

# Tabela dos tipos de estabelecimento
class TPUNID(Base):
    __tablename__ = 'tpunid'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos turnos de funcionamento do estabelecimento
class TURNOAT(Base):
    __tablename__ = 'turnoat'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TURNO = Column(String(66))                                                  # Logical key

# Tabela dos níveis de atendimento do estabelecimento
class NIVHIER(Base):
    __tablename__ = 'nivhier'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NIVEL = Column(String(66))                                                  # Logical key

# Tabela dos tipos de prestador dos serviços hospitalares
class TPPREST(Base):
    __tablename__ = 'tpprest'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    PRESTADOR = Column(String(66))                                              # Logical key

# Tabela dos tipos de estabelecimento (só o próprio filantrópico na verdade)
class SGRUPHAB(Base):
    __tablename__ = 'sgruphab'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela das naturezas jurídicas de estabelecimentos
class NATJUR(Base):
    __tablename__ = 'natjur'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(100))                                              # Logical key

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes_ef'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes_ef'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
