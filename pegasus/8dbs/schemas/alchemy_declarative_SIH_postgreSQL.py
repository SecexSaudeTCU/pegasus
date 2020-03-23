###########################################################################################################################################################################
# SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do SIH para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""


# Incorpora o schema do banco de dados do SIH definido nas classes abaixo
Base = declarative_base()

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela dos municípios
class UFZI(Base):
    __tablename__ = 'ufzi'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sih.ufcod.ID'))                    # Foreign key
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

# Tabela dos tipos de procedimento solicitado
class PROCSOLIC(Base):
    __tablename__ = 'procsolic'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(10), primary_key=True)                                   # Primary key
    PROCEDIMENTO = Column(String(100))                                          # Logical key

# Tabela dos tipos de diagnóstico principal
class DIAGPRINC(Base):
    __tablename__ = 'diagprinc'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    DIAGNOSTICO = Column(String(66))                                            # Logical key

# Tabela das ocupações dos pacientes
class CBOR(Base):
    __tablename__ = 'cbor'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(7), primary_key=True)                                    # Primary key
    DESCESTAB = Column(String(66))                                              # Logical key

# Tabela dos níveis de complexidade de atendimento
class COMPLEX(Base):
    __tablename__ = 'complex'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    COMPLEXIDADE = Column(String(66))                                           # Logical key

# Tabela dos tipos de recursos
class FINANC(Base):
    __tablename__ = 'financ'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    FONTE = Column(String(66))                                                  # Logical key

# Tabela dos tipos de recursos FAEC
class FAECTP(Base):
    __tablename__ = 'faectp'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    SUBFONTE = Column(String(66))                                               # Logical key

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'sih'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo


###########################################################################################################################################################################
# SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD #
###########################################################################################################################################################################

# Tabela das AIH Reduzidas (RD)
class RDBR(Base):
    __tablename__ = 'rdbr'
    __table_args__ = {'schema': 'sih'}
    # Aqui se define as colunas para a tabela rdbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    N_AIH = Column(String(13))                                                  # Logical key
    UF_RD = Column(String(2))                                                   # Atributo
    ANO_RD = Column(Integer)                                                    # Atributo
    MES_RD = Column(String(2))                                                  # Atributo
    IDENT_ID = Column(String(2), ForeignKey('sih.ident.ID'))                    # Foreign key
    ident = relationship('IDENT')
    UFZI_ID = Column(String(6), ForeignKey('sih.ufzi.ID'))                      # Foreign key
    ufzi = relationship('UFZI')
    ANO_CMPT = Column(Integer)                                                  # Atributo
    MES_CMPT = Column(String(2))                                                # Atributo
    ESPEC_ID = Column(String(2), ForeignKey('sih.espec.ID'))                    # Foreign key
    espec = relationship('ESPEC')
    CGC_HOSP = Column(String(14))                                               # Atributo
    IDENT_ID = Column(String(2), ForeignKey('sih.ident.ID'))                    # Foreign key
    ident = relationship('IDENT')
    CEP = Column(String(8))                                                     # Atributo
    MUNICRES_ID = Column(String(6), ForeignKey('sih.ufzi.ID'))                  # Foreign key
    ufzi = relationship('UFZI')
    NASC = Column(Date)                                                         # Atributo
    SEXO = Column(String(2))                                                    # Atributo
    UTI_MES_TO = Column(Float)                                                  # Atributo
    MARCAUTI_ID = Column(String(2), ForeignKey('sih.marcauti.ID'))              # Foreign key
    marcauti = relationship('MARCAUTI')
    UTI_INT_TO = Column(Float)                                                  # Atributo
    DIAR_ACOM = Column(Float)                                                   # Atributo
    QT_DIARIAS = Column(Float)                                                  # Atributo
    PROCSOLIC_ID = Column(String(10), ForeignKey('sih.procsolic.ID'))           # Foreign key
    procsolic = relationship('PROCSOLIC')
    PROCREA_ID = Column(String(10), ForeignKey('sih.procsolic.ID'))             # Foreign key
    procsolic = relationship('PROCSOLIC')
    VAL_SH = Column(Float)                                                      # Atributo
    VAL_SP = Column(Float)                                                      # Atributo
    VAL_TOT = Column(Float)                                                     # Atributo
    VAL_UTI = Column(Float)                                                     # Atributo
    US_TOT = Column(Float)                                                      # Atributo
    DI_INTER = Column(Date)                                                     # Atributo
    DT_SAIDA = Column(Date)                                                     # Atributo
    DIAGPRINC_ID = Column(String(4), ForeignKey('sih.diagprinc.ID'))            # Foreign key
    diagprinc = relationship('DIAGPRINC')
    COBRANCA_ID = Column(String(4), ForeignKey('sih.cobranca.ID'))              # Foreign key
    cobranca = relationship('COBRANCA')
    NATUREZA_ID = Column(String(4), ForeignKey('sih.natureza.ID'))              # Foreign key
    natureza = relationship('NATUREZA')
    NATJUR_ID = Column(String(4), ForeignKey('sih.natjur.ID'))                  # Foreign key
    natjur = relationship('NATJUR')
    GESTAO_ID = Column(String(4), ForeignKey('sih.gestao.ID'))                  # Foreign key
    gestao = relationship('GESTAO')
    IND_VDRL = Column(Numeric)                                                  # Atributo
    MUNICMOV_ID = Column(String(6), ForeignKey('sih.ufzi.ID'))                  # Foreign key
    ufzi = relationship('UFZI')
    IDADE = Column(Float)                                                       # Atributo
    DIAS_PERM = Column(Float)                                                   # Atributo
    MORTE = Column(Numeric)                                                     # Atributo
    NACIONAL_ID = Column(String(3), ForeignKey('sih.nacional.ID'))              # Foreign key
    nacional = relationship('NACIONAL')
    CARINT_ID = Column(String(2), ForeignKey('sih.carint.ID'))                  # Foreign key
    carint = relationship('CARINT')
    HOMONIMO = Column(String(2))                                                # Atributo
    NUM_FILHOS = Column(Float)                                                  # Atributo
    INSTRU_ID = Column(String(2), ForeignKey('sih.instru.ID'))                  # Foreign key
    instru = relationship('INSTRU')
    CID_NOTIF = Column(String(4))                                               # Atributo
    CONTRACEP1_ID = Column(String(2), ForeignKey('sih.contracep1.ID'))          # Foreign key
    contracep1 = relationship('CONTRACEP1')
    CONTRACEP2_ID = Column(String(2), ForeignKey('sih.contracep1.ID'))          # Foreign key
    contracep1 = relationship('CONTRACEP1')
    GESTRISCO = Column(Numeric)                                                 # Atributo
    INSC_PN = Column(String(12))                                                # Atributo
    CBOR_ID = Column(String(6), ForeignKey('sih.cbor.ID'))                      # Foreign key
    cbor = relationship('CBOR')
    CNAER_ID = Column(String(3), ForeignKey('sih.cnaer.ID'))                    # Foreign key
    cnaer = relationship('CNAER')
    VINCPREV_ID = Column(String(2), ForeignKey('sih.vincprev.ID'))              # Foreign key
    vincprev = relationship('VINCPREV')
    GESTOR_TP = Column(String(2))                                               # Atributo
    GESTOR_CPF = Column(String(11))                                             # Atributo
    CNES_ID = Column(String(7), ForeignKey('sih.cnes.ID'))                      # Foreign key
    cnes = relationship('CNES')
    CNPJ_MANT = Column(String(14))                                              # Atributo
    INFEHOSP = Column(Numeric)                                                  # Atributo
    CID_ASSO = Column(String(4))                                                # Atributo
    CID_MORTE = Column(String(4))                                               # Atributo
    COMPLEX_ID = Column(String(2), ForeignKey('sih.complex.ID'))                # Foreign key
    complex = relationship('COMPLEX')
    FINANC_ID = Column(String(2), ForeignKey('sih.financ.ID'))                  # Foreign key
    financ = relationship('FINANC')
    FAECTP_ID = Column(String(6), ForeignKey('sih.faectp.ID'))                  # Foreign key
    faectp = relationship('FAECTP')
    REGCT_ID = Column(String(4), ForeignKey('sih.regct.ID'))                    # Foreign key
    regct = relationship('REGCT')
    RACACOR_ID = Column(String(2), ForeignKey('sih.racacor.ID'))                # Foreign key
    racacor = relationship('RACACOR')
    ETNIA_ID = Column(String(4), ForeignKey('sih.etnia.ID'))                    # Foreign key
    etnia = relationship('ETNIA')
    AUD_JUST = Column(String(50))                                               # Atributo
    SIS_JUST = Column(String(50))                                               # Atributo
    VAL_SH_FED = Column(Float)                                                  # Atributo
    VAL_SP_FED = Column(Float)                                                  # Atributo
    VAL_SH_GES = Column(Float)                                                  # Atributo
    VAL_SP_GES = Column(Float)                                                  # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de AIH
class IDENT(Base):
    __tablename__ = 'ident'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO_AIH = Column(String(66))                                               # Logical key

# Tabela dos tipos de leito
class ESPEC(Base):
    __tablename__ = 'espec'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    LEITO = Column(String(66))                                                  # Logical key

# Tabela dos tipos de UTI
class MARCAUTI(Base):
    __tablename__ = 'marcauti'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO_UTI = Column(String(66))                                               # Logical key

# Tabela dos motivos de saída/permanência
class COBRANCA(Base):
    __tablename__ = 'cobranca'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    MOTIVO = Column(String(66))                                                 # Logical key

# Tabela dos tipos de natureza jurídica dos hospitais
class NATUREZA(Base):
    __tablename__ = 'natureza'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(66))                                               # Logical key

# Tabela das naturezas jurídicas de estabelecimentos
class NATJUR(Base):
    __tablename__ = 'natjur'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(100))                                              # Logical key

# Tabela das naturezas jurídicas de estabelecimentos
class GESTAO(Base):
    __tablename__ = 'gestao'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    GESTAO = Column(String(66))                                                 # Logical key

# Tabela das nacionalidades dos pacientes
class NACIONAL(Base):
    __tablename__ = 'nacional'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    NACIONALIDADE = Column(String(66))                                          # Logical key

# Tabela dos motivos de internação
class CARINT(Base):
    __tablename__ = 'carint'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    MOTIVO = Column(String(66))                                                 # Logical key

# Tabela dos motivos de internação
class INSTRU(Base):
    __tablename__ = 'instru'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    NIVEL = Column(String(66))                                                  # Logical key

# Tabela dos tipos de contraceptivo (primeiro)
class CONTRACEP1(Base):
    __tablename__ = 'contracep1'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CONTRACEPTIVO = Column(String(66))                                          # Logical key

# Tabela das atividades econômicas
class CNAER(Base):
    __tablename__ = 'cnaer'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    ATIVIDADE = Column(String(66))                                              # Logical key

# Tabela dos tipos de vínculo previdenciário do paciente
class VINCPREV(Base):
    __tablename__ = 'vincprev'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    VINCULO = Column(String(66))                                                # Logical key

# Tabela dos tipos de regras contratuais
class REGCT(Base):
    __tablename__ = 'regct'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    REGRA = Column(String(66))                                                  # Logical key

# Tabela dos tipos de raça do paciente
class RACACOR(Base):
    __tablename__ = 'racacor'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    RACA = Column(String(66))                                                   # Logical key

# Tabela dos tipos de etnia indígena
class ETNIA(Base):
    __tablename__ = 'etnia'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    INDIGENA = Column(String(66))                                               # Logical key

###########################################################################################################################################################################
# SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP #
###########################################################################################################################################################################

# Tabela das AIH Serviços Profissionais (SP)
class SPBR(Base):
    __tablename__ = 'spbr'
    __table_args__ = {'schema': 'sih'}
    # Aqui se define as colunas para a tabela spbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    SP_NAIH = Column(String(13))                                                # Logical key
    SPPROCREA_ID = Column(String(10), ForeignKey('sih.procsolic.ID'))           # Foreign key
    procsolic = relationship('PROCSOLIC')
    UF_SP = Column(String(2))                                                   # Atributo
    ANO_SP = Column(Integer)                                                    # Atributo
    MES_SP = Column(String(2))                                                  # Atributo
    SPGESTOR_ID = Column(String(6), ForeignKey('sih.ufzi.ID'))                  # Foreign key
    ufzi = relationship('UFZI')
    SPCNES_ID = Column(String(7), ForeignKey('sih.cnes.ID'))                    # Foreign key
    cnes = relationship('CNES')
    SP_DTINTER = Column(Date)                                                   # Atributo
    SP_DTSAIDA = Column(Date)                                                   # Atributo
    SP_CPFCGC = Column(String(14))                                              # Atributo
    SPATOPROF_ID = Column(String(10), ForeignKey('sih.procsolic.ID'))           # Foreign key
    procsolic = relationship('PROCSOLIC')
    SP_QTD_ATO = Column(Float)                                                  # Atributo
    SP_PTSP = Column(Float)                                                     # Atributo
    SP_VALATO = Column(Float)                                                   # Atributo
    SPMHOSP_ID = Column(String(6), ForeignKey('sih.ufzi.ID'))                   # Foreign key
    ufzi = relationship('UFZI')
    SPMPAC_ID = Column(String(6), ForeignKey('sih.ufzi.ID'))                    # Foreign key
    ufzi = relationship('UFZI')
    SP_DES_HOS = Column(Numeric)                                                # Atributo
    SP_DES_PAC = Column(Numeric)                                                # Atributo
    SPCOMPLEX_ID = Column(String(2), ForeignKey('sih.complex.ID'))              # Foreign key
    complex = relationship('COMPLEX')
    SPFINANC_ID = Column(String(2), ForeignKey('sih.financ.ID'))                # Foreign key
    financ = relationship('FINANC')
    SPCOFAEC_ID = Column(String(6), ForeignKey('sih.faectp.ID'))                # Foreign key
    faectp = relationship('FAECTP')
    SPPFCBO_ID = Column(String(6), ForeignKey('sih.cbor.ID'))                   # Foreign key
    cbor = relationship('CBOR')
    SP_PF_DOC = Column(String(11))                                              # Atributo
    SP_PJ_DOC = Column(String(7))                                               # Atributo
    INTPVAL_ID = Column(String(2), ForeignKey('sih.intpval.ID'))                # Foreign key
    intpval = relationship('INTPVAL')
    SERVCLA_ID = Column(String(6), ForeignKey('sih.servcla.ID'))                # Foreign key
    servcla = relationship('SERVCLA')
    SPCIDPRI_ID = Column(String(4), ForeignKey('sih.diagprinc.ID'))             # Foreign key
    diagprinc = relationship('DIAGPRINC')
    SPCIDSEC_ID = Column(String(4), ForeignKey('sih.diagprinc.ID'))             # Foreign key
    diagprinc = relationship('DIAGPRINC')
    SP_QT_PROC = Column(Float)                                                  # Atributo
    SP_U_AIH = Column(Numeric)                                                  # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos tipos de recursos
class INTPVAL(Base):
    __tablename__ = 'intpval'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO_VALOR = Column(String(66))                                             # Logical key

# Tabela de classificações dos serviços
class SERVCLA(Base):
    __tablename__ = 'servcla'
    __table_args__ = {'schema': 'sih'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    CLASSIFICACAO = Column(String(100))                                         # Logical key
