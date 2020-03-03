###########################################################################################################################################################
# CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST #
###########################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES_ST (Estabelecimentos) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e Microsoft SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do CNES_ST definido nas classes abaixo
Base = declarative_base()

# Tabela dos Estabelecimentos (ST) (tabela principal do banco de dados)
class STBR(Base):
    __tablename__ = 'stbr'
    __table_args__ = {'schema': 'cnes_st'}
    # Aqui se define as colunas para a tabela "stbr" (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes_st.cnes.ID'))                  # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_ST = Column(String(2))                                                   # Atributo
    ANO_ST = Column(Integer)                                                    # Atributo
    MES_ST = Column(String(2))                                                  # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes_st.codufmun.ID'))          # Foreign key
    codufmun = relationship('CODUFMUN')
    COD_CEP = Column(String(8))                                                 # Atributo
    CPF_CNPJ = Column(String(14))                                               # Atributo
    PFPJ_ID = Column(String(2), ForeignKey('cnes_st.pfpj.ID'))                  # Foreign key
    pfpj = relationship('PFPJ')
    NIVDEP_ID = Column(String(2), ForeignKey('cnes_st.nivdep.ID'))              # Foreign key
    nivdep = relationship('NIVDEP')
    CNPJ_MAN = Column(String(14))                                               # Atributo
    CODIR_ID = Column(String(2), ForeignKey('cnes_st.codir.ID'))                # Foreign key
    codir = relationship('CODIR')
    VINC_SUS = Column(Numeric)                                                  # Atributo
    TPGESTAO_ID = Column(String(2), ForeignKey('cnes_st.tpgestao.ID'))          # Foreign key
    tpgestao = relationship('TPGESTAO')
    ESFERAA_ID = Column(String(2), ForeignKey('cnes_st.esferaa.ID'))            # Foreign key
    esferaa = relationship('ESFERAA')
    RETENCAO_ID = Column(String(2), ForeignKey('cnes_st.retencao.ID'))          # Foreign key
    retencao = relationship('RETENCAO')
    ATIVIDAD_ID = Column(String(2), ForeignKey('cnes_st.atividad.ID'))          # Foreign key
    atividad = relationship('ATIVIDAD')
    NATUREZA_ID = Column(String(2), ForeignKey('cnes_st.natureza.ID'))          # Foreign key
    natureza = relationship('NATUREZA')
    CLIENTEL_ID = Column(String(2), ForeignKey('cnes_st.clientel.ID'))          # Foreign key
    clientel = relationship('CLIENTEL')
    TPUNID_ID = Column(String(2), ForeignKey('cnes_st.tpunid.ID'))              # Foreign key
    tpunid = relationship('TPUNID')
    TURNOAT_ID = Column(String(2), ForeignKey('cnes_st.turnoat.ID'))            # Foreign key
    turnoat = relationship('TURNOAT')
    NIVHIER_ID = Column(String(2), ForeignKey('cnes_st.nivhier.ID'))            # Foreign key
    nivhier = relationship('NIVHIER')
    TPPREST_ID = Column(String(2), ForeignKey('cnes_st.tpprest.ID'))            # Foreign key
    tpprest = relationship('TPPREST')
    CO_BANCO = Column(String(3))                                                # Atributo
    CO_AGENC = Column(String(5))                                                # Atributo
    C_CORREN = Column(String(14))                                               # Atributo
    ALVARA = Column(String(25))                                                 # Atributo
    DT_EXPED = Column(Date)                                                     # Atributo
    ORGEXPED_ID = Column(String(2), ForeignKey('cnes_st.orgexped.ID'))          # Foreign key
    orgexped = relationship('ORGEXPED')
    AV_ACRED = Column(Numeric)                                                  # Atributo
    CLASAVAL_ID = Column(String(2), ForeignKey('cnes_st.clasaval.ID'))          # Foreign key
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
    NATJUR_ID = Column(String(4), ForeignKey('cnes_st.natjur.ID'))              # Foreign key
    natjur = relationship('NATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes_st'}
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
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes_st.ufcod.ID'))                # Foreign key
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
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    PESSOA = Column(String(66))                                                 # Logical key

# Tabela do grau de independência do estabelecimento
class NIVDEP(Base):
    __tablename__ = 'nivdep'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos tipos de retenção de tributos da mantenedora
class CODIR(Base):
    __tablename__ = 'codir'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    RETENCAO = Column(String(66))                                               # Logical key

# Tabela dos tipos de gestão
class TPGESTAO(Base):
    __tablename__ = 'tpgestao'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    GESTAO = Column(String(66))                                                 # Logical key

# Tabela dos tipos de administração
class ESFERAA(Base):
    __tablename__ = 'esferaa'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ADMINISTRACAO = Column(String(66))                                          # Logical key

# Tabela dos tipos de retenção de tributos do estabelecimento
class RETENCAO(Base):
    __tablename__ = 'retencao'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    RETENCAO = Column(String(66))                                               # Logical key

# Tabela dos tipos de atividade de ensino, se houver
class ATIVIDAD(Base):
    __tablename__ = 'atividad'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ATIVIDADE = Column(String(66))                                              # Logical key

# Tabela da natureza do estabelecimento
class NATUREZA(Base):
    __tablename__ = 'natureza'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(66))                                               # Logical key

# Tabela dos tipos de fluxo de clientela
class CLIENTEL(Base):
    __tablename__ = 'clientel'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CLIENTELA = Column(String(66))                                              # Logical key

# Tabela dos tipos de estabelecimento
class TPUNID(Base):
    __tablename__ = 'tpunid'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos turnos de funcionamento do estabelecimento
class TURNOAT(Base):
    __tablename__ = 'turnoat'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TURNO = Column(String(66))                                                  # Logical key

# Tabela dos níveis de atendimento do estabelecimento
class NIVHIER(Base):
    __tablename__ = 'nivhier'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NIVEL = Column(String(66))                                                  # Logical key

# Tabela dos tipos de prestador dos serviços hospitalares
class TPPREST(Base):
    __tablename__ = 'tpprest'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    PRESTADOR = Column(String(66))                                              # Logical key

# Tabela dos tipos de órgão expedidor de alvará
class ORGEXPED(Base):
    __tablename__ = 'orgexped'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    EXPEDIDOR = Column(String(66))                                              # Logical key

# Tabela das classificações de avaliacao de estabelecimentos
class CLASAVAL(Base):
    __tablename__ = 'clasaval'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    AVALIACAO = Column(String(66))                                              # Logical key

# Tabela das naturezas jurídicas de estabelecimentos
class NATJUR(Base):
    __tablename__ = 'natjur'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(100))                                              # Logical key

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes_st'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes_st'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
