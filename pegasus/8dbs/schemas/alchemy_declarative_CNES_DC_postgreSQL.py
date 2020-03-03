###########################################################################################################################################################
# CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC #
###########################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do CNES_DC (Dados Complementares) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do CNES_DC definido nas classes abaixo
Base = declarative_base()

# Tabela dos Dados Complementares (DC) (tabela principal do "sub" banco de dados CNES_DC)
class DCBR(Base):
    __tablename__ = 'dcbr'
    __table_args__ = {'schema': 'cnes_dc'}
    # Aqui se define as colunas para a tabela "dcbr" (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    CNES_ID = Column(String(7), ForeignKey('cnes_dc.cnes.ID'))                  # Primary (should be!) and Foreign key
    cnes = relationship('CNES')
    UF_DC = Column(String(2))                                                   # Atributo
    ANO_DC = Column(Integer)                                                    # Atributo
    MES_DC = Column(String(2),)                                                 # Atributo
    CODUFMUN_ID = Column(String(6), ForeignKey('cnes_dc.codufmun.ID'))          # Atributo
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

# Tabela dos estabelecimentos de saúde
class CNES(Base):
    __tablename__ = 'cnes'
    __table_args__ = {'schema': 'cnes_dc'}
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
    __table_args__ = {'schema': 'cnes_dc'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('cnes_dc.ufcod.ID'))                # Foreign key
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

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'cnes_dc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'cnes_dc'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
