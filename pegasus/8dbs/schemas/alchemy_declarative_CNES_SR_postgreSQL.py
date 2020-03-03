###########################################################################################################################################################################
# CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES_SR (Serviço Especializado) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do CNES_SR definido nas classes abaixo
Base = declarative_base()

# Tabela Serviço Especializado (SR) (tabela principal do "sub" banco de dados CNES_SR)
class SRBR(Base):
    __tablename__ = 'srbr'
    __table_args__ = {'schema': 'cnes_sr'}
    # Aqui se define as colunas para a tabela srbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes_sr.cnes.ID'))                  # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_SR = Column(String(2))                                                   # Atributo
    ANO_SR = Column(Integer)                                                    # Atributo
    MES_SR = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes_sr.codufmun.ID'))          # Foreign key
    codufmun = relationship('CODUFMUN')
    SERVESP_ID = Column(String(3), ForeignKey('cnes_sr.servesp.ID'))            # Logical/Foreign key
    servesp = relationship('SERVESP')
    CLASSSR_ID = Column(String(6), ForeignKey('cnes_sr.classsr.ID'))            # Logical/Foreign key
    classsr = relationship('CLASSSR')
    SRVUNICO_ID = Column(String(3), ForeignKey('cnes_sr.srvunico.ID'))          # Logical/Foreign key
    srvunico = relationship('SRVUNICO')
    TPGESTAO_ID = Column(String(2), ForeignKey('cnes_sr.tpgestao.ID'))          # Foreign key
    tpgestao = relationship('TPGESTAO')
    PFPJ_ID = Column(String(2), ForeignKey('cnes_sr.pfpj.ID'))                  # Foreign key
    pfpj = relationship('PFPJ')
    CPF_CNPJ = Column(String(14))                                               # Atributo
    NIVDEP_ID = Column(String(2), ForeignKey('cnes_sr.nivdep.ID'))              # Foreign key
    nivdep = relationship('NIVDEP')
    ESFERAA_ID = Column(String(2), ForeignKey('cnes_sr.esferaa.ID'))            # Foreign key
    esferaa = relationship('ESFERAA')
    ATIVIDAD_ID = Column(String(2), ForeignKey('cnes_sr.atividad.ID'))          # Foreign key
    atividad = relationship('ATIVIDAD')
    RETENCAO_ID = Column(String(2), ForeignKey('cnes_sr.retencao.ID'))          # Foreign key
    retencao = relationship('RETENCAO')
    NATUREZA_ID = Column(String(2), ForeignKey('cnes_sr.natureza.ID'))          # Foreign key
    natureza = relationship('NATUREZA')
    CLIENTEL_ID = Column(String(2), ForeignKey('cnes_sr.clientel.ID'))          # Foreign key
    clientel = relationship('CLIENTEL')
    TPUNID_ID = Column(String(2), ForeignKey('cnes_sr.tpunid.ID'))              # Foreign key
    tpunid = relationship('TPUNID')
    TURNOAT_ID = Column(String(2), ForeignKey('cnes_sr.turnoat.ID'))            # Foreign key
    turnoat = relationship('TURNOAT')
    NIVHIER_ID = Column(String(2), ForeignKey('cnes_sr.nivhier.ID'))            # Foreign key
    nivhier = relationship('NIVHIER')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    CNPJ_MAN = Column(String(14))                                               # Atributo
    CARACTER_ID = Column(String(2), ForeignKey('cnes_sr.caracter.ID'))          # Foreign key
    caracter = relationship('CARACTER')
    AMB_NSUS = Column(Numeric)                                                  # Atributo
    AMB_SUS  = Column(Numeric)                                                  # Atributo
    HOSP_NSUS = Column(Numeric)                                                 # Atributo
    HOSP_SUS  = Column(Numeric)                                                 # Atributo
    CONTSRVU  = Column(Numeric)                                                 # Atributo
    CNESTERC = Column(String(7))                                                # Atributo
    NATJUR_ID = Column(String(4), ForeignKey('cnes_sr.natjur.ID'))              # Foreign key
    natjur = relationship('NATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes_sr'}
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
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes_sr.ufcod.ID'))                # Foreign key
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

# Tabela dos serviços especializados
class SERVESP(Base):
    __tablename__ = 'servesp'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(100))                                             # Logical key

# Tabela de classificações dos serviços especializados
class CLASSSR(Base):
    __tablename__ = 'classsr'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(100))                                             # Logical key

# Na verdade é a mesma coisa da tabela servesp
class SRVUNICO(Base):
    __tablename__ = 'srvunico'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(100))                                             # Logical key

# Tabela dos tipos de gestão
class TPGESTAO(Base):
    __tablename__ = 'tpgestao'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    GESTAO = Column(String(66))                                                 # Logical key

# Tabela se o estabelecimento é pessoa física ou pessoa jurídica
class PFPJ(Base):
    __tablename__ = 'pfpj'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    PESSOA = Column(String(66))                                                 # Logical key

# Tabela do grau de independência do estabelecimento
class NIVDEP(Base):
    __tablename__ = 'nivdep'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos tipos de administração
class ESFERAA(Base):
    __tablename__ = 'esferaa'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ADMINISTRACAO = Column(String(66))                                          # Logical key

# Tabela dos tipos de atividade de ensino, se houver
class ATIVIDAD(Base):
    __tablename__ = 'atividad'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ATIVIDADE = Column(String(66))                                              # Logical key

# Tabela dos tipos de retenção de tributos do estabelecimento
class RETENCAO(Base):
    __tablename__ = 'retencao'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    RETENCAO = Column(String(66))                                               # Logical key

# Tabela da natureza do estabelecimento
class NATUREZA(Base):
    __tablename__ = 'natureza'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(66))                                               # Logical key

# Tabela dos tipos de fluxo de clientela
class CLIENTEL(Base):
    __tablename__ = 'clientel'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CLIENTELA = Column(String(66))                                              # Logical key

# Tabela dos tipos de estabelecimento
class TPUNID(Base):
    __tablename__ = 'tpunid'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos turnos de funcionamento do estabelecimento
class TURNOAT(Base):
    __tablename__ = 'turnoat'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TURNO = Column(String(66))                                                  # Logical key

# Tabela dos níveis de atendimento do estabelecimento
class NIVHIER(Base):
    __tablename__ = 'nivhier'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NIVEL = Column(String(66))                                                  # Logical key

# Tabela de caracterizações do estabelecimento
class CARACTER(Base):
    __tablename__ = 'caracter'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CARACTERIZACAO = Column(String(66))                                         # Logical key

# Tabela das naturezas jurídicas de estabelecimentos
class NATJUR(Base):
    __tablename__ = 'natjur'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(100))                                              # Logical key

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes_sr'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes_sr'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
