###########################################################################################################################################################################
#  CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES   #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""


# Incorpora o schema do banco de dados do CNES definido nas classes abaixo
Base = declarative_base()

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(7), primary_key=True)                                    # Primary key
    DESCESTAB = Column(String(66))                                              # Logical key
    RSOC_MAN = Column(String(66))                                               # Atributo
    CPF_CNPJ = Column(String(14))                                               # Atributo
    EXCLUIDO = Column(Numeric)                                                  # Atributo
    DATAINCL = Column(Date)                                                     # Atributo
    DATAEXCL = Column(Date)                                                     # Atributo

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela dos municípios de localização de estabelecimentos
class CODUFMUN(Base):
    __tablename__ = 'codufmun'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes.ufcod.ID'))                   # Foreign key
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

# Tabela se o estabelecimento é pessoa física ou pessoa jurídica
class PFPJ(Base):
    __tablename__ = 'pfpj'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    PESSOA = Column(String(66))                                                 # Logical key

# Tabela do grau de independência do estabelecimento
class NIVDEP(Base):
    __tablename__ = 'nivdep'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos tipos de retenção de tributos da mantenedora
class CODIR(Base):
    __tablename__ = 'codir'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    RETENCAO = Column(String(66))                                               # Logical key

# Tabela dos tipos de gestão
class TPGESTAO(Base):
    __tablename__ = 'tpgestao'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    GESTAO = Column(String(66))                                                 # Logical key

# Tabela dos tipos de administração
class ESFERAA(Base):
    __tablename__ = 'esferaa'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ADMINISTRACAO = Column(String(66))                                          # Logical key

# Tabela dos tipos de retenção de tributos do estabelecimento
class RETENCAO(Base):
    __tablename__ = 'retencao'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    RETENCAO = Column(String(66))                                               # Logical key

# Tabela dos tipos de atividade de ensino, se houver
class ATIVIDAD(Base):
    __tablename__ = 'atividad'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ATIVIDADE = Column(String(66))                                              # Logical key

# Tabela da natureza do estabelecimento
class NATUREZA(Base):
    __tablename__ = 'natureza'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(66))                                               # Logical key

# Tabela dos tipos de fluxo de clientela
class CLIENTEL(Base):
    __tablename__ = 'clientel'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CLIENTELA = Column(String(66))                                              # Logical key

# Tabela dos tipos de estabelecimento
class TPUNID(Base):
    __tablename__ = 'tpunid'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos turnos de funcionamento do estabelecimento
class TURNOAT(Base):
    __tablename__ = 'turnoat'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TURNO = Column(String(66))                                                  # Logical key

# Tabela dos níveis de atendimento do estabelecimento
class NIVHIER(Base):
    __tablename__ = 'nivhier'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NIVEL = Column(String(66))                                                  # Logical key

# Tabela dos tipos de prestador dos serviços hospitalares
class TPPREST(Base):
    __tablename__ = 'tpprest'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    PRESTADOR = Column(String(66))                                              # Logical key

# Tabela das naturezas jurídicas de estabelecimentos
class NATJUR(Base):
    __tablename__ = 'natjur'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(100))                                              # Logical key

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo


###########################################################################################################################################################################
# CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST #
###########################################################################################################################################################################

# Tabela dos Estabelecimentos (ST)
class STBR(Base):
    __tablename__ = 'stbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se definem as colunas para a tabela stbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_ST = Column(String(2))                                                   # Atributo
    ANO_ST = Column(Integer)                                                    # Atributo
    MES_ST = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    COD_CEP = Column(String(8))                                                 # Atributo
    CPF_CNPJ = Column(String(14))                                               # Atributo
    PFPJ_ID = Column(String(2), ForeignKey('cnes.pfpj.ID'))                     # Foreign key
    pfpj = relationship('PFPJ')
    NIVDEP_ID = Column(String(2), ForeignKey('cnes.nivdep.ID'))                 # Foreign key
    nivdep = relationship('NIVDEP')
    CNPJ_MAN = Column(String(14))                                               # Atributo
    CODIR_ID = Column(String(2), ForeignKey('cnes.codir.ID'))                   # Foreign key
    codir = relationship('CODIR')
    VINC_SUS = Column(Numeric)                                                  # Atributo
    TPGESTAO_ID = Column(String(2), ForeignKey('cnes.tpgestao.ID'))             # Foreign key
    tpgestao = relationship('TPGESTAO')
    ESFERAA_ID = Column(String(2), ForeignKey('cnes.esferaa.ID'))               # Foreign key
    esferaa = relationship('ESFERAA')
    RETENCAO_ID = Column(String(2), ForeignKey('cnes.retencao.ID'))             # Foreign key
    retencao = relationship('RETENCAO')
    ATIVIDAD_ID = Column(String(2), ForeignKey('cnes.atividad.ID'))             # Foreign key
    atividad = relationship('ATIVIDAD')
    NATUREZA_ID = Column(String(2), ForeignKey('cnes.natureza.ID'))             # Foreign key
    natureza = relationship('NATUREZA')
    CLIENTEL_ID = Column(String(2), ForeignKey('cnes.clientel.ID'))             # Foreign key
    clientel = relationship('CLIENTEL')
    TPUNID_ID = Column(String(2), ForeignKey('cnes.tpunid.ID'))                 # Foreign key
    tpunid = relationship('TPUNID')
    TURNOAT_ID = Column(String(2), ForeignKey('cnes.turnoat.ID'))               # Foreign key
    turnoat = relationship('TURNOAT')
    NIVHIER_ID = Column(String(2), ForeignKey('cnes.nivhier.ID'))               # Foreign key
    nivhier = relationship('NIVHIER')
    TPPREST_ID = Column(String(2), ForeignKey('cnes.tpprest.ID'))               # Foreign key
    tpprest = relationship('TPPREST')
    CO_BANCO = Column(String(3))                                                # Atributo
    CO_AGENC = Column(String(5))                                                # Atributo
    C_CORREN = Column(String(14))                                               # Atributo
    ALVARA = Column(String(25))                                                 # Atributo
    DT_EXPED = Column(Date)                                                     # Atributo
    ORGEXPED_ID = Column(String(2), ForeignKey('cnes.orgexped.ID'))             # Foreign key
    orgexped = relationship('ORGEXPED')
    AV_ACRED = Column(Numeric)                                                  # Atributo
    CLASAVAL_ID = Column(String(2), ForeignKey('cnes.clasaval.ID'))             # Foreign key
    clasaval = relationship('CLASAVAL')
    DT_ACRED = Column(Date)                                                     # Atributo
    AV_PNASS = Column(Numeric)                                                  # Atributo
    DT_PNASS = Column(Date)                                                     # Atributo
    GESPRG1E = Column(Numeric)                                                  # Atributo
    GESPRG1M = Column(Numeric)                                                  # Atributo
    GESPRG2E = Column(Numeric)                                                  # Atributo
    GESPRG2M = Column(Numeric)                                                  # Atributo
    GESPRG4E = Column(Numeric)                                                  # Atributo
    GESPRG4M = Column(Numeric)                                                  # Atributo
    NIVATE_A = Column(Numeric)                                                  # Atributo
    GESPRG3E = Column(Numeric)                                                  # Atributo
    GESPRG3M = Column(Numeric)                                                  # Atributo
    GESPRG5E = Column(Numeric)                                                  # Atributo
    GESPRG5M = Column(Numeric)                                                  # Atributo
    GESPRG6E = Column(Numeric)                                                  # Atributo
    GESPRG6M = Column(Numeric)                                                  # Atributo
    NIVATE_H = Column(Numeric)                                                  # Atributo
    QTLEITP1 = Column(Float)                                                    # Atributo
    QTLEITP2 = Column(Float)                                                    # Atributo
    QTLEITP3 = Column(Float)                                                    # Atributo
    LEITHOSP = Column(Numeric)                                                  # Atributo
    QTINST01 = Column(Float)                                                    # Atributo
    QTINST02 = Column(Float)                                                    # Atributo
    QTINST03 = Column(Float)                                                    # Atributo
    QTINST04 = Column(Float)                                                    # Atributo
    QTINST05 = Column(Float)                                                    # Atributo
    QTINST06 = Column(Float)                                                    # Atributo
    QTINST07 = Column(Float)                                                    # Atributo
    QTINST08 = Column(Float)                                                    # Atributo
    QTINST09 = Column(Float)                                                    # Atributo
    QTINST10 = Column(Float)                                                    # Atributo
    QTINST11 = Column(Float)                                                    # Atributo
    QTINST12 = Column(Float)                                                    # Atributo
    QTINST13 = Column(Float)                                                    # Atributo
    QTINST14 = Column(Float)                                                    # Atributo
    URGEMERG = Column(Numeric)                                                  # Atributo
    QTINST15 = Column(Float)                                                    # Atributo
    QTINST16 = Column(Float)                                                    # Atributo
    QTINST17 = Column(Float)                                                    # Atributo
    QTINST18 = Column(Float)                                                    # Atributo
    QTINST19 = Column(Float)                                                    # Atributo
    QTINST20 = Column(Float)                                                    # Atributo
    QTINST21 = Column(Float)                                                    # Atributo
    QTINST22 = Column(Float)                                                    # Atributo
    QTINST23 = Column(Float)                                                    # Atributo
    QTINST24 = Column(Float)                                                    # Atributo
    QTINST25 = Column(Float)                                                    # Atributo
    QTINST26 = Column(Float)                                                    # Atributo
    QTINST27 = Column(Float)                                                    # Atributo
    QTINST28 = Column(Float)                                                    # Atributo
    QTINST29 = Column(Float)                                                    # Atributo
    QTINST30 = Column(Float)                                                    # Atributo
    ATENDAMB = Column(Numeric)                                                  # Atributo
    QTINST31 = Column(Float)                                                    # Atributo
    QTINST32 = Column(Float)                                                    # Atributo
    QTINST33 = Column(Float)                                                    # Atributo
    CENTRCIR = Column(Numeric)                                                  # Atributo
    QTINST34 = Column(Float)                                                    # Atributo
    QTINST35 = Column(Float)                                                    # Atributo
    QTINST36 = Column(Float)                                                    # Atributo
    QTINST37 = Column(Float)                                                    # Atributo
    CENTROBS = Column(Numeric)                                                  # Atributo
    QTLEIT05 = Column(Float)                                                    # Atributo
    QTLEIT06 = Column(Float)                                                    # Atributo
    QTLEIT07 = Column(Float)                                                    # Atributo
    QTLEIT09 = Column(Float)                                                    # Atributo
    QTLEIT19 = Column(Float)                                                    # Atributo
    QTLEIT20 = Column(Float)                                                    # Atributo
    QTLEIT21 = Column(Float)                                                    # Atributo
    QTLEIT22 = Column(Float)                                                    # Atributo
    QTLEIT23 = Column(Float)                                                    # Atributo
    QTLEIT32 = Column(Float)                                                    # Atributo
    QTLEIT34 = Column(Float)                                                    # Atributo
    QTLEIT38 = Column(Float)                                                    # Atributo
    QTLEIT39 = Column(Float)                                                    # Atributo
    QTLEIT40 = Column(Float)                                                    # Atributo
    CENTRNEO = Column(Numeric)                                                  # Atributo
    ATENDHOS = Column(Numeric)                                                  # Atributo
    SERAP01P = Column(Numeric)                                                  # Atributo
    SERAP01T = Column(Numeric)                                                  # Atributo
    SERAP02P = Column(Numeric)                                                  # Atributo
    SERAP02T = Column(Numeric)                                                  # Atributo
    SERAP03P = Column(Numeric)                                                  # Atributo
    SERAP03T = Column(Numeric)                                                  # Atributo
    SERAP04P = Column(Numeric)                                                  # Atributo
    SERAP04T = Column(Numeric)                                                  # Atributo
    SERAP05P = Column(Numeric)                                                  # Atributo
    SERAP05T = Column(Numeric)                                                  # Atributo
    SERAP06P = Column(Numeric)                                                  # Atributo
    SERAP06T = Column(Numeric)                                                  # Atributo
    SERAP07P = Column(Numeric)                                                  # Atributo
    SERAP07T = Column(Numeric)                                                  # Atributo
    SERAP08P = Column(Numeric)                                                  # Atributo
    SERAP08T = Column(Numeric)                                                  # Atributo
    SERAP09P = Column(Numeric)                                                  # Atributo
    SERAP09T = Column(Numeric)                                                  # Atributo
    SERAP10P = Column(Numeric)                                                  # Atributo
    SERAP10T = Column(Numeric)                                                  # Atributo
    SERAP11P = Column(Numeric)                                                  # Atributo
    SERAP11T = Column(Numeric)                                                  # Atributo
    SERAPOIO = Column(Numeric)                                                  # Atributo
    RES_BIOL = Column(Numeric)                                                  # Atributo
    RES_QUIM = Column(Numeric)                                                  # Atributo
    RES_RADI = Column(Numeric)                                                  # Atributo
    RES_COMU = Column(Numeric)                                                  # Atributo
    COLETRES = Column(Numeric)                                                  # Atributo
    COMISS01 = Column(Numeric)                                                  # Atributo
    COMISS02 = Column(Numeric)                                                  # Atributo
    COMISS03 = Column(Numeric)                                                  # Atributo
    COMISS04 = Column(Numeric)                                                  # Atributo
    COMISS05 = Column(Numeric)                                                  # Atributo
    COMISS06 = Column(Numeric)                                                  # Atributo
    COMISS07 = Column(Numeric)                                                  # Atributo
    COMISS08 = Column(Numeric)                                                  # Atributo
    COMISS09 = Column(Numeric)                                                  # Atributo
    COMISS10 = Column(Numeric)                                                  # Atributo
    COMISS11 = Column(Numeric)                                                  # Atributo
    COMISS12 = Column(Numeric)                                                  # Atributo
    COMISSAO = Column(Numeric)                                                  # Atributo
    AP01CV01 = Column(Numeric)                                                  # Atributo
    AP01CV02 = Column(Numeric)                                                  # Atributo
    AP01CV05 = Column(Numeric)                                                  # Atributo
    AP01CV06 = Column(Numeric)                                                  # Atributo
    AP01CV03 = Column(Numeric)                                                  # Atributo
    AP01CV04 = Column(Numeric)                                                  # Atributo
    AP02CV01 = Column(Numeric)                                                  # Atributo
    AP02CV02 = Column(Numeric)                                                  # Atributo
    AP02CV05 = Column(Numeric)                                                  # Atributo
    AP02CV06 = Column(Numeric)                                                  # Atributo
    AP02CV03 = Column(Numeric)                                                  # Atributo
    AP02CV04 = Column(Numeric)                                                  # Atributo
    AP03CV01 = Column(Numeric)                                                  # Atributo
    AP03CV02 = Column(Numeric)                                                  # Atributo
    AP03CV05 = Column(Numeric)                                                  # Atributo
    AP03CV06 = Column(Numeric)                                                  # Atributo
    AP03CV03 = Column(Numeric)                                                  # Atributo
    AP03CV04 = Column(Numeric)                                                  # Atributo
    AP04CV01 = Column(Numeric)                                                  # Atributo
    AP04CV02 = Column(Numeric)                                                  # Atributo
    AP04CV05 = Column(Numeric)                                                  # Atributo
    AP04CV06 = Column(Numeric)                                                  # Atributo
    AP04CV03 = Column(Numeric)                                                  # Atributo
    AP04CV04 = Column(Numeric)                                                  # Atributo
    AP05CV01 = Column(Numeric)                                                  # Atributo
    AP05CV02 = Column(Numeric)                                                  # Atributo
    AP05CV05 = Column(Numeric)                                                  # Atributo
    AP05CV06 = Column(Numeric)                                                  # Atributo
    AP05CV03 = Column(Numeric)                                                  # Atributo
    AP05CV04 = Column(Numeric)                                                  # Atributo
    AP06CV01 = Column(Numeric)                                                  # Atributo
    AP06CV02 = Column(Numeric)                                                  # Atributo
    AP06CV05 = Column(Numeric)                                                  # Atributo
    AP06CV06 = Column(Numeric)                                                  # Atributo
    AP06CV03 = Column(Numeric)                                                  # Atributo
    AP06CV04 = Column(Numeric)                                                  # Atributo
    AP07CV01 = Column(Numeric)                                                  # Atributo
    AP07CV02 = Column(Numeric)                                                  # Atributo
    AP07CV05 = Column(Numeric)                                                  # Atributo
    AP07CV06 = Column(Numeric)                                                  # Atributo
    AP07CV03 = Column(Numeric)                                                  # Atributo
    AP07CV04 = Column(Numeric)                                                  # Atributo
    ATEND_PR = Column(Numeric)                                                  # Atributo
    NATJUR_ID = Column(String(4), ForeignKey('cnes.natjur.ID'))                 # Foreign key
    natjur = relationship('NATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de órgão expedidor de alvará
class ORGEXPED(Base):
    __tablename__ = 'orgexped'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    EXPEDIDOR = Column(String(66))                                              # Logical key

# Tabela das classificações de avaliacao de estabelecimentos
class CLASAVAL(Base):
    __tablename__ = 'clasaval'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    AVALIACAO = Column(String(66))                                              # Logical key


###########################################################################################################################################################################
# CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC #
###########################################################################################################################################################################

# Tabela dos Dados Complementares (DC)
class DCBR(Base):
    __tablename__ = 'dcbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela dcbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_DC = Column(String(2))                                                   # Atributo
    ANO_DC = Column(Integer)                                                    # Atributo
    MES_DC = Column(String(2),)                                                 # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    S_HBSAGP = Column(Float)                                                    # Atributo
    S_HBSAGN = Column(Float)                                                    # Atributo
    S_DPI = Column(Float)                                                       # Atributo
    S_DPAC = Column(Float)                                                      # Atributo
    S_REAGP = Column(Float)                                                     # Atributo
    S_REAGN = Column(Float)                                                     # Atributo
    S_REHCV = Column(Float)                                                     # Atributo
    MAQ_PROP = Column(Float)                                                    # Atributo
    MAQ_OUTR = Column(Float)                                                    # Atributo
    F_AREIA = Column(Numeric)                                                   # Atributo
    F_CARVAO = Column(Numeric)                                                  # Atributo
    ABRANDAD = Column(Numeric)                                                  # Atributo
    DEIONIZA = Column(Numeric)                                                  # Atributo
    OSMOSE_R = Column(Numeric)                                                  # Atributo
    OUT_TRAT = Column(Numeric)                                                  # Atributo
    CNS_NEFR = Column(String(15))                                               # Atributo
    DIALISE = Column(Numeric)                                                   # Atributo
    SIMUL_RD = Column(Float)                                                    # Atributo
    PLANJ_RD = Column(Float)                                                    # Atributo
    ARMAZ_FT = Column(Float)                                                    # Atributo
    CONF_MAS = Column(Float)                                                    # Atributo
    SALA_MOL = Column(Float)                                                    # Atributo
    BLOCOPER = Column(Float)                                                    # Atributo
    S_ARMAZE = Column(Float)                                                    # Atributo
    S_PREPAR = Column(Float)                                                    # Atributo
    S_QCDURA = Column(Float)                                                    # Atributo
    S_QLDURA = Column(Float)                                                    # Atributo
    S_CPFLUX = Column(Float)                                                    # Atributo
    S_SIMULA = Column(Float)                                                    # Atributo
    S_ACELL6 = Column(Float)                                                    # Atributo
    S_ALSEME = Column(Float)                                                    # Atributo
    S_ALCOME = Column(Float)                                                    # Atributo
    ORTV1050 = Column(Float)                                                    # Atributo
    ORV50150 = Column(Float)                                                    # Atributo
    OV150500 = Column(Float)                                                    # Atributo
    UN_COBAL = Column(Float)                                                    # Atributo
    EQBRBAIX = Column(Float)                                                    # Atributo
    EQBRMEDI = Column(Float)                                                    # Atributo
    EQBRALTA = Column(Float)                                                    # Atributo
    EQ_MAREA = Column(Float)                                                    # Atributo
    EQ_MINDI = Column(Float)                                                    # Atributo
    EQSISPLN = Column(Float)                                                    # Atributo
    EQDOSCLI = Column(Float)                                                    # Atributo
    EQFONSEL = Column(Float)                                                    # Atributo
    CNS_ADM = Column(String(15))                                                # Atributo
    CNS_OPED = Column(String(15))                                               # Atributo
    CNS_CONC = Column(String(15))                                               # Atributo
    CNS_OCLIN = Column(String(15))                                              # Atributo
    CNS_MRAD = Column(String(15))                                               # Atributo
    CNS_FNUC = Column(String(15))                                               # Atributo
    QUIMRADI = Column(Numeric)                                                  # Atributo
    S_RECEPC = Column(Float)                                                    # Atributo
    S_TRIHMT = Column(Float)                                                    # Atributo
    S_TRICLI = Column(Float)                                                    # Atributo
    S_COLETA = Column(Float)                                                    # Atributo
    S_AFERES = Column(Float)                                                    # Atributo
    S_PREEST = Column(Float)                                                    # Atributo
    S_PROCES = Column(Float)                                                    # Atributo
    S_ESTOQU = Column(Float)                                                    # Atributo
    S_DISTRI = Column(Float)                                                    # Atributo
    S_SOROLO = Column(Float)                                                    # Atributo
    S_IMUNOH = Column(Float)                                                    # Atributo
    S_PRETRA = Column(Float)                                                    # Atributo
    S_HEMOST = Column(Float)                                                    # Atributo
    S_CONTRQ = Column(Float)                                                    # Atributo
    S_BIOMOL = Column(Float)                                                    # Atributo
    S_IMUNFE = Column(Float)                                                    # Atributo
    S_TRANSF = Column(Float)                                                    # Atributo
    S_SGDOAD = Column(Float)                                                    # Atributo
    QT_CADRE = Column(Float)                                                    # Atributo
    QT_CENRE = Column(Float)                                                    # Atributo
    QT_REFSA = Column(Float)                                                    # Atributo
    QT_CONRA = Column(Float)                                                    # Atributo
    QT_EXTPL = Column(Float)                                                    # Atributo
    QT_FRE18 = Column(Float)                                                    # Atributo
    QT_FRE30 = Column(Float)                                                    # Atributo
    QT_AGIPL = Column(Float)                                                    # Atributo
    QT_SELAD = Column(Float)                                                    # Atributo
    QT_IRRHE = Column(Float)                                                    # Atributo
    QT_AGLTN = Column(Float)                                                    # Atributo
    QT_MAQAF = Column(Float)                                                    # Atributo
    QT_REFRE = Column(Float)                                                    # Atributo
    QT_REFAS = Column(Float)                                                    # Atributo
    QT_CAPFL = Column(Float)                                                    # Atributo
    CNS_HMTR = Column(String(15))                                               # Atributo
    CNS_HMTL = Column(String(15))                                               # Atributo
    CNS_CRES = Column(String(15))                                               # Atributo
    CNS_RTEC = Column(String(15))                                               # Atributo
    HEMOTERA = Column(Numeric)                                                  # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key


###########################################################################################################################################################################
# CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF #
###########################################################################################################################################################################

# Tabela dos Profissionais (PF)
class PFBR(Base):
    __tablename__ = 'pfbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela pfbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_PF = Column(String(2))                                                   # Atributo
    ANO_PF = Column(Integer)                                                    # Atributo
    MES_PF = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    CBO_ID = Column(String(6), ForeignKey('cnes.cbo.ID'))                       # Logical/Foreign key
    cbo = relationship('CBO')
    CBOUNICO_ID = Column(String(6), ForeignKey('cnes.cbounico.ID'))             # Foreign key
    cbounico = relationship('CBOUNICO')
    NOMEPROF = Column(String(60))                                               # Logical key
    CNS_PROF = Column(String(15))                                               # Logical key
    CONSELHO_ID = Column(String(2), ForeignKey('cnes.conselho.ID'))             # Foreign key
    conselho = relationship('CONSELHO')
    REGISTRO = Column(String(13))                                               # Atributo
    VINCULAC_ID = Column(String(6), ForeignKey('cnes.vinculac.ID'))             # Logical/Foreign key
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

# Tabela das especialidades dos profissionais
class CBO(Base):
    __tablename__ = 'cbo'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Em verdade é a mesma coisa da tabela cbo
class CBOUNICO(Base):
    __tablename__ = 'cbounico'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Tabeça dos conselhos de profissão
class CONSELHO(Base):
    __tablename__ = 'conselho'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    DENOMINACAO = Column(String(66))                                            # Logical key

# Tabeça dos tipos de vínculos do profissional
class VINCULAC(Base):
    __tablename__ = 'vinculac'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(100))                                             # Logical key


###########################################################################################################################################################################
# CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT #
###########################################################################################################################################################################

# Tabela dos Leitos (LT)
class LTBR(Base):
    __tablename__ = 'ltbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela ltbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_LT = Column(String(2))                                                   # Atributo
    ANO_LT = Column(Integer)                                                    # Atributo
    MES_LT = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    TPLEITO_ID = Column(String(6), ForeignKey('cnes.tpleito.ID'))               # Logical/Foreign key
    tpleito = relationship('TPLEITO')
    CODLEITO_ID = Column(String(6), ForeignKey('cnes.codleito.ID'))             # Logical/Foreign key
    codleito = relationship('CODLEITO')
    QT_EXIST = Column(Float)                                                    # Logical key
    QT_CONTR = Column(Float)                                                    # Logical key
    QT_SUS   = Column(Float)                                                    # Logical key
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de leito do estabelecimento
class TPLEITO(Base):
    __tablename__ = 'tpleito'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos tipos específicos dos leitos
class CODLEITO(Base):
    __tablename__ = 'codleito'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESPECIALIDADE = Column(String(66))                                          # Logical key


###########################################################################################################################################################################
# CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ #
###########################################################################################################################################################################

# Tabela dos Equipamentos (EQ)
class EQBR(Base):
    __tablename__ = 'eqbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela eqbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_EQ = Column(String(2))                                                   # Atributo
    ANO_EQ = Column(Integer)                                                    # Atributo
    MES_EQ = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    TIPEQUIP_ID = Column(String(2), ForeignKey('cnes.tipequip.ID'))             # Logical/Foreign key
    tipequip = relationship('TIPEQUIP')
    CODEQUIP_ID = Column(String(2), ForeignKey('cnes.codequip.ID'))             # Logical/Foreign key
    codequip = relationship('CODEQUIP')
    QT_EXIST = Column(Float)                                                    # Logical key
    QT_USO = Column(Float)                                                      # Logical key
    IND_SUS = Column(Numeric)                                                   # Atributo
    ND_NSUS = Column(Numeric)                                                   # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de equipamentos
class TIPEQUIP(Base):
    __tablename__ = 'tipequip'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos nomes dos equipamentos
class CODEQUIP(Base):
    __tablename__ = 'codequip'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    DENOMINACAO = Column(String(66))                                            # Logical key


###########################################################################################################################################################################
# CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR #
###########################################################################################################################################################################

# Tabela do Serviço Especializado (SR) (tabela principal do "sub" banco de dados CNES_SR)
class SRBR(Base):
    __tablename__ = 'srbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela srbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_SR = Column(String(2))                                                   # Atributo
    ANO_SR = Column(Integer)                                                    # Atributo
    MES_SR = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    SERVESP_ID = Column(String(3), ForeignKey('cnes.servesp.ID'))               # Logical/Foreign key
    servesp = relationship('SERVESP')
    CLASSSR_ID = Column(String(6), ForeignKey('cnes.classsr.ID'))               # Logical/Foreign key
    classsr = relationship('CLASSSR')
    SRVUNICO_ID = Column(String(3), ForeignKey('cnes.srvunico.ID'))             # Logical/Foreign key
    srvunico = relationship('SRVUNICO')
    TPGESTAO_ID = Column(String(2), ForeignKey('cnes.tpgestao.ID'))             # Foreign key
    tpgestao = relationship('TPGESTAO')
    PFPJ_ID = Column(String(2), ForeignKey('cnes.pfpj.ID'))                     # Foreign key
    pfpj = relationship('PFPJ')
    CPF_CNPJ = Column(String(14))                                               # Atributo
    NIVDEP_ID = Column(String(2), ForeignKey('cnes.nivdep.ID'))                 # Foreign key
    nivdep = relationship('NIVDEP')
    ESFERAA_ID = Column(String(2), ForeignKey('cnes.esferaa.ID'))               # Foreign key
    esferaa = relationship('ESFERAA')
    ATIVIDAD_ID = Column(String(2), ForeignKey('cnes.atividad.ID'))             # Foreign key
    atividad = relationship('ATIVIDAD')
    RETENCAO_ID = Column(String(2), ForeignKey('cnes.retencao.ID'))             # Foreign key
    retencao = relationship('RETENCAO')
    NATUREZA_ID = Column(String(2), ForeignKey('cnes.natureza.ID'))             # Foreign key
    natureza = relationship('NATUREZA')
    CLIENTEL_ID = Column(String(2), ForeignKey('cnes.clientel.ID'))             # Foreign key
    clientel = relationship('CLIENTEL')
    TPUNID_ID = Column(String(2), ForeignKey('cnes.tpunid.ID'))                 # Foreign key
    tpunid = relationship('TPUNID')
    TURNOAT_ID = Column(String(2), ForeignKey('cnes.turnoat.ID'))               # Foreign key
    turnoat = relationship('TURNOAT')
    NIVHIER_ID = Column(String(2), ForeignKey('cnes.nivhier.ID'))               # Foreign key
    nivhier = relationship('NIVHIER')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    CNPJ_MAN = Column(String(14))                                               # Atributo
    CARACTER_ID = Column(String(2), ForeignKey('cnes.caracter.ID'))             # Foreign key
    caracter = relationship('CARACTER')
    AMB_NSUS = Column(Numeric)                                                  # Atributo
    AMB_SUS  = Column(Numeric)                                                  # Atributo
    HOSP_NSUS = Column(Numeric)                                                 # Atributo
    HOSP_SUS  = Column(Numeric)                                                 # Atributo
    CONTSRVU  = Column(Numeric)                                                 # Atributo
    CNESTERC = Column(String(7))                                                # Atributo
    NATJUR_ID = Column(String(4), ForeignKey('cnes.natjur.ID'))                 # Foreign key
    natjur = relationship('NATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos serviços especializados
class SERVESP(Base):
    __tablename__ = 'servesp'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(100))                                             # Logical key

# Tabela de classificações dos serviços especializados
class CLASSSR(Base):
    __tablename__ = 'classsr'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(100))                                             # Logical key

# Na verdade é a mesma coisa da tabela servesp
class SRVUNICO(Base):
    __tablename__ = 'srvunico'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(100))                                             # Logical key

# Tabela de caracterizações do estabelecimento
class CARACTER(Base):
    __tablename__ = 'caracter'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CARACTERIZACAO = Column(String(66))                                         # Logical key


###########################################################################################################################################################################
# CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP #
###########################################################################################################################################################################

# Tabela das Equipes (EP)
class EPBR(Base):
    __tablename__ = 'epbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela eqbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_EP = Column(String(2))                                                   # Atributo
    ANO_EP = Column(Integer)                                                    # Atributo
    MES_EP = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    IDEQUIPE_ID = Column(String(18), ForeignKey('cnes.idequipe.ID'))            # Foreign key
    idequipe = relationship('IDEQUIPE')
    TIPOEQP_ID = Column(String(2), ForeignKey('cnes.tipoeqp.ID'))               # Foreign key
    tipoeqp = relationship('TIPOEQP')
    NOME_EQP = Column(String(60))                                               # Atributo
    IDAREA_ID = Column(String(10), ForeignKey('cnes.idarea.ID'))                # Foreign key
    idarea = relationship('IDAREA')
    NOMEAREA = Column(String(60))                                               # Atributo
    IDSEGM_ID = Column(String(8), ForeignKey('cnes.idsegm.ID'))                 # Foreign key
    idsegm = relationship('IDSEGM')
    DESCSEGM = Column(String(60))                                               # Atributo
    TIPOSEGM_ID = Column(String(2), ForeignKey('cnes.tiposegm.ID'))             # Foreign key
    tiposegm = relationship('TIPOSEGM')
    DT_ATIVA = Column(Date)                                                     # Atributo
    DT_DESAT = Column(Date)                                                     # Atributo
    QUILOMBO = Column(Numeric)                                                  # Atributo
    ASSENTAD = Column(Numeric)                                                  # Atributo
    POPGERAL = Column(Numeric)                                                  # Atributo
    ESCOLA = Column(Numeric)                                                    # Atributo
    INDIGENA = Column(Numeric)                                                  # Atributo
    PRONASCI = Column(Numeric)                                                  # Atributo
    MOTDESAT_ID = Column(String(2), ForeignKey('cnes.motdesat.ID'))             # Foreign key
    motdesat = relationship('MOTDESAT')
    TPDESAT_ID = Column(String(2), ForeignKey('cnes.tpdesat.ID'))               # Foreign key
    tpdesat = relationship('TPDESAT')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabelas da identificação de equipes
class IDEQUIPE(Base):
    __tablename__ = 'idequipe'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(18), primary_key=True)                                   # Primary key
    NOME_EQUIPE = Column(String(66))                                            # Logical key

# Tabela dos tipos de equipe
class TIPOEQP(Base):
    __tablename__ = 'tipoeqp'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela da identificação de áreas
class IDAREA(Base):
    __tablename__ = 'idarea'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(10), primary_key=True)                                   # Primary key
    NOME_AREA = Column(String(66))                                              # Logical key

# Tabela da identificação de segmentos
class IDSEGM(Base):
    __tablename__ = 'idsegm'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(8), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(66))                                              # Logical key

# Tabela dos tipos de segmento
class TIPOSEGM(Base):
    __tablename__ = 'tiposegm'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos motivos de desativação
class MOTDESAT(Base):
    __tablename__ = 'motdesat'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    MOTIVO = Column(String(66))                                                 # Logical key

# Tabela dos tipos de desativação
class TPDESAT(Base):
    __tablename__ = 'tpdesat'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key


###########################################################################################################################################################################
# CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB #
###########################################################################################################################################################################

# Tabela das Habilitações (HB)
class HBBR(Base):
    __tablename__ = 'hbbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela hbbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_HB = Column(String(2))                                                   # Atributo
    ANO_HB = Column(Integer)                                                    # Atributo
    MES_HB = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    SGRUPHAB_ID = Column(String(4), ForeignKey('cnes.sgruphab_hb.ID'))          # Foreign key
    sgruphab_hb = relationship('SGRUPHAB_HB')
    CMPT_INI = Column(Date)                                                     # Atributo
    CMPT_FIM = Column(Date)                                                     # Atributo
    DTPORTAR = Column(Date)                                                     # Atributo
    PORTARIA = Column(String(50))                                               # Atributo
    MAPORTAR = Column(Date)                                                     # Atributo
    NULEITOS = Column(Float)                                                    # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de habilitação
class SGRUPHAB_HB(Base):
    __tablename__ = 'sgruphab_hb'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    HABILITACAO = Column(String(100))                                           # Logical key


###########################################################################################################################################################################
# CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC #
###########################################################################################################################################################################

# Tabela das Regras Contratuais (RC)
class RCBR(Base):
    __tablename__ = 'rcbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela rcbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_RC = Column(String(2))                                                   # Atributo
    ANO_RC = Column(Integer)                                                    # Atributo
    MES_RC = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    SGRUPHAB_ID = Column(String(4), ForeignKey('cnes.sgruphab_rc.ID'))          # Foreign key
    sgruphab_rc = relationship('SGRUPHAB_RC')
    CMPT_INI = Column(Date)                                                     # Atributo
    CMPT_FIM = Column(Date)                                                     # Atributo
    DTPORTAR = Column(Date)                                                     # Atributo
    PORTARIA = Column(String(50))                                               # Atributo
    MAPORTAR = Column(Date)                                                     # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de regra contratual
class SGRUPHAB_RC(Base):
    __tablename__ = 'sgruphab_rc'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    REGRA = Column(String(150))                                                 # Logical key


###########################################################################################################################################################################
# CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM #
###########################################################################################################################################################################

# Tabela de Gestão e Metas (GM)
class GMBR(Base):
    __tablename__ = 'gmbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela gmbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_GM = Column(String(2))                                                   # Atributo
    ANO_GM = Column(Integer)                                                    # Atributo
    MES_GM = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    SGRUPHAB_ID = Column(String(4), ForeignKey('cnes.sgruphab_gm.ID'))          # Foreign key
    sgruphab_gm = relationship('SGRUPHAB_GM')
    CMPT_INI = Column(Date)                                                     # Atributo
    CMPT_FIM = Column(Date)                                                     # Atributo
    DTPORTAR = Column(Date)                                                     # Atributo
    PORTARIA = Column(String(50))                                               # Atributo
    MAPORTAR = Column(Date)                                                     # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de gestão
class SGRUPHAB_GM(Base):
    __tablename__ = 'sgruphab_gm'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    GESTAO = Column(String(100))                                                # Logical key


###########################################################################################################################################################################
# CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE #
###########################################################################################################################################################################

# Tabela dos Estabelecimentos de Ensino (EE)
class EEBR(Base):
    __tablename__ = 'eebr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela eebr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_EE = Column(String(2))                                                   # Atributo
    ANO_EE = Column(Integer)                                                    # Atributo
    MES_EE = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    TPGESTAO_ID = Column(String(2), ForeignKey('cnes.tpgestao.ID'))             # Foreign key
    tpgestao = relationship('TPGESTAO')
    PFPJ_ID = Column(String(2), ForeignKey('cnes.pfpj.ID'))                     # Foreign key
    pfpj = relationship('PFPJ')
    CPF_CNPJ = Column(String(14))                                               # Atributo
    NIVDEP_ID = Column(String(2), ForeignKey('cnes.nivdep.ID'))                 # Foreign key
    nivdep = relationship('NIVDEP')
    CNPJ_MAN = Column(String(14))                                               # Atributo
    ESFERAA_ID = Column(String(2), ForeignKey('cnes.esferaa.ID'))               # Foreign key
    esferaa = relationship('ESFERAA')
    RETENCAO_ID = Column(String(2), ForeignKey('cnes.retencao.ID'))             # Foreign key
    retencao = relationship('RETENCAO')
    ATIVIDAD_ID = Column(String(2), ForeignKey('cnes.atividad.ID'))             # Foreign key
    atividad = relationship('ATIVIDAD')
    NATUREZA_ID = Column(String(2), ForeignKey('cnes.natureza.ID'))             # Foreign key
    natureza = relationship('NATUREZA')
    CLIENTEL_ID = Column(String(2), ForeignKey('cnes.clientel.ID'))             # Foreign key
    clientel = relationship('CLIENTEL')
    TPUNID_ID = Column(String(2), ForeignKey('cnes.tpunid.ID'))                 # Foreign key
    tpunid = relationship('TPUNID')
    TURNOAT_ID = Column(String(2), ForeignKey('cnes.turnoat.ID'))               # Foreign key
    turnoat = relationship('TURNOAT')
    NIVHIER_ID = Column(String(2), ForeignKey('cnes.nivhier.ID'))               # Foreign key
    nivhier = relationship('NIVHIER')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    COD_CEP = Column(String(8))                                                 # Atributo
    VINC_SUS = Column(Numeric)                                                  # Atributo
    TPPREST_ID = Column(String(2), ForeignKey('cnes.tpprest.ID'))               # Foreign key
    tpprest = relationship('TPPREST')
    SGRUPHAB_ID = Column(String(4), ForeignKey('cnes.sgruphab_ee.ID'))          # Foreign key
    sgruphab_ee = relationship('SGRUPHAB_EE')
    CMPT_INI = Column(Date)                                                     # Atributo
    CMPT_FIM = Column(Date)                                                     # Atributo
    DTPORTAR = Column(Date)                                                     # Atributo
    PORTARIA = Column(String(50))                                               # Atributo
    MAPORTAR = Column(Date)                                                     # Atributo
    NATJUR_ID = Column(String(4), ForeignKey('cnes.natjur.ID'))                 # Foreign key
    natjur = relationship('NATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de estabelecimento (só o próprio de ensino na verdade)
class SGRUPHAB_EE(Base):
    __tablename__ = 'sgruphab_ee'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key


###########################################################################################################################################################################
# CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF #
###########################################################################################################################################################################

# Tabela dos Estabelecimentos Filantrópicos (EF) (tabela principal do "sub" banco de dados CNES_EF)
class EFBR(Base):
    __tablename__ = 'efbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela efbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_EF = Column(String(2))                                                   # Atributo
    ANO_EF = Column(Integer)                                                    # Atributo
    MES_EF = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    TPGESTAO_ID = Column(String(2), ForeignKey('cnes.tpgestao.ID'))             # Foreign key
    tpgestao = relationship('TPGESTAO')
    PFPJ_ID = Column(String(2), ForeignKey('cnes.pfpj.ID'))                     # Foreign key
    pfpj = relationship('PFPJ')
    CPF_CNPJ = Column(String(14))                                               # Atributo
    NIVDEP_ID = Column(String(2), ForeignKey('cnes.nivdep.ID'))                 # Foreign key
    nivdep = relationship('NIVDEP')
    CNPJ_MAN = Column(String(14))                                               # Atributo
    ESFERAA_ID = Column(String(2), ForeignKey('cnes.esferaa.ID'))               # Foreign key
    esferaa = relationship('ESFERAA')
    RETENCAO_ID = Column(String(2), ForeignKey('cnes.retencao.ID'))             # Foreign key
    retencao = relationship('RETENCAO')
    ATIVIDAD_ID = Column(String(2), ForeignKey('cnes.atividad.ID'))             # Foreign key
    atividad = relationship('ATIVIDAD')
    NATUREZA_ID = Column(String(2), ForeignKey('cnes.natureza.ID'))             # Foreign key
    natureza = relationship('NATUREZA')
    CLIENTEL_ID = Column(String(2), ForeignKey('cnes.clientel.ID'))             # Foreign key
    clientel = relationship('CLIENTEL')
    TPUNID_ID = Column(String(2), ForeignKey('cnes.tpunid.ID'))                 # Foreign key
    tpunid = relationship('TPUNID')
    TURNOAT_ID = Column(String(2), ForeignKey('cnes.turnoat.ID'))               # Foreign key
    turnoat = relationship('TURNOAT')
    NIVHIER_ID = Column(String(2), ForeignKey('cnes.nivhier.ID'))               # Foreign key
    nivhier = relationship('NIVHIER')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    COD_CEP = Column(String(8))                                                 # Atributo
    VINC_SUS = Column(Numeric)                                                  # Atributo
    TPPREST_ID = Column(String(2), ForeignKey('cnes.tpprest.ID'))               # Foreign key
    tpprest = relationship('TPPREST')
    SGRUPHAB_ID = Column(String(4), ForeignKey('cnes.sgruphab_ef.ID'))          # Foreign key
    sgruphab_ef = relationship('SGRUPHAB_EF')
    CMPT_INI = Column(Date)                                                     # Atributo
    CMPT_FIM = Column(Date)                                                     # Atributo
    DTPORTAR = Column(Date)                                                     # Atributo
    PORTARIA = Column(String(50))                                               # Atributo
    MAPORTAR = Column(Date)                                                     # Atributo
    NATJUR_ID = Column(String(4), ForeignKey('cnes.natjur.ID'))                 # Foreign key
    natjur = relationship('NATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de estabelecimento (só o próprio filantrópico na verdade)
class SGRUPHAB_EF(Base):
    __tablename__ = 'sgruphab_ef'
    __table_args__ = {'schema': 'cnes'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key


###########################################################################################################################################################################
# CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN #
###########################################################################################################################################################################

# Tabela dos Incentivos (IN)
class INBR(Base):
    __tablename__ = 'inbr'
    __table_args__ = {'schema': 'cnes'}
    # Aqui se define as colunas para a tabela inbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes.cnes.ID'))                     # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_IN = Column(String(2))                                                   # Atributo
    ANO_IN = Column(Integer)                                                    # Atributo
    MES_IN = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes.codufmun.ID'))             # Foreign key
    codufmun = relationship('CODUFMUN')
    TPGESTAO_ID = Column(String(2), ForeignKey('cnes.tpgestao.ID'))             # Foreign key
    tpgestao = relationship('TPGESTAO')
    PFPJ_ID = Column(String(2), ForeignKey('cnes.pfpj.ID'))                     # Foreign key
    pfpj = relationship('PFPJ')
    CPF_CNPJ = Column(String(14))                                               # Atributo
    NIVDEP_ID = Column(String(2), ForeignKey('cnes.nivdep.ID'))                 # Foreign key
    nivdep = relationship('NIVDEP')
    CNPJ_MAN = Column(String(14))                                               # Atributo
    ESFERAA_ID = Column(String(2), ForeignKey('cnes.esferaa.ID'))               # Foreign key
    esferaa = relationship('ESFERAA')
    RETENCAO_ID = Column(String(2), ForeignKey('cnes.retencao.ID'))             # Foreign key
    retencao = relationship('RETENCAO')
    ATIVIDAD_ID = Column(String(2), ForeignKey('cnes.atividad.ID'))             # Foreign key
    atividad = relationship('ATIVIDAD')
    NATUREZA_ID = Column(String(2), ForeignKey('cnes.natureza.ID'))             # Foreign key
    natureza = relationship('NATUREZA')
    CLIENTEL_ID = Column(String(2), ForeignKey('cnes.clientel.ID'))             # Foreign key
    clientel = relationship('CLIENTEL')
    TPUNID_ID = Column(String(2), ForeignKey('cnes.tpunid.ID'))                 # Foreign key
    tpunid = relationship('TPUNID')
    TURNOAT_ID = Column(String(2), ForeignKey('cnes.turnoat.ID'))               # Foreign key
    turnoat = relationship('TURNOAT')
    NIVHIER_ID = Column(String(2), ForeignKey('cnes.nivhier.ID'))               # Foreign key
    nivhier = relationship('NIVHIER')
    TERCEIRO = Column(Numeric)                                                  # Atributo
    COD_CEP = Column(String(8))                                                 # Atributo
    VINC_SUS = Column(Numeric)                                                  # Atributo
    TPPREST_ID = Column(String(2), ForeignKey('cnes.tpprest.ID'))               # Foreign key
    tpprest = relationship('TPPREST')
    SGRUPHAB = Column(String(4))                                                # Blurred
    CMPT_INI = Column(Date)                                                     # Atributo
    CMPT_FIM = Column(Date)                                                     # Atributo
    DTPORTAR = Column(Date)                                                     # Atributo
    PORTARIA = Column(String(50))                                               # Atributo
    MAPORTAR = Column(Date)                                                     # Atributo
    NATJUR_ID = Column(String(4), ForeignKey('cnes.natjur.ID'))                 # Foreign key
    natjur = relationship('NATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key
